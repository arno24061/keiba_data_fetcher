import win32com.client
import logging
import threading

class JRAVanFetcher:
    def __init__(self):
        try: self.jv = win32com.client.Dispatch("JVDTLab.JVLink")
        except: self.jv = None
        
    def init_link(self): 
        return self.jv is not None and self.jv.JVInit("UNKNOWN") == 0

    def open_rt(self, spec, key):
        res = self.jv.JVRTOpen(spec, key)
        return int(res[0] if isinstance(res, tuple) else res) if str(res[0] if isinstance(res, tuple) else res).strip() else -1
    
    def read_rt(self, b, s, f):
        r = self.jv.JVRead(b, s, f)
        c, d = (r[0], r[1]) if isinstance(r, tuple) else (r, "")
        try: return int(c), d
        except: return -1, ""
        
    def close_rt(self): 
        self.jv.JVClose()

    def get_today_places(self, today_str, stop_event: threading.Event):
        places = []
        logging.info(f"[JRA-VAN] 0B01(開催スケジュール)から本日の開催場を動的抽出します...")
        try:
            res = self.open_rt("0B01", today_str)
            if res >= 0:
                b, s, f = "", 200000, ""
                while True:
                    if stop_event.is_set(): break
                    c, d = self.read_rt(b, s, f)
                    if c > 0 and d:
                        for line in d.splitlines():
                            if len(line) >= 21 and line.startswith("YS"):
                                place_code = line[19:21]
                                if place_code.isdigit():
                                    places.append(int(place_code))
                    elif c <= 0: break
                self.close_rt()
        except Exception as e:
            logging.error(f"[JRA-VAN] 0B01 読み込みエラー: {e}")
            try: self.close_rt()
            except: pass
            
        places = sorted(list(set(places)))
        if not places:
            logging.warning("[JRA-VAN] 開催場コードを取得できませんでした。フェイルセーフとして総当たり(1-10)を実行します。")
            return list(range(1, 11))
            
        logging.info(f"[JRA-VAN] 本日の開催場コードを特定: {places}")
        return places

    def fetch_rt_loop(self, specs, today_str, places, source_name, stop_event: threading.Event):
        data = []
        for spec in specs:
            if stop_event.is_set(): break
            logging.info(f"[{source_name}] >> {spec} の速報データを取得中...")
            
            try:
                res = self.open_rt(spec, today_str)
                if res >= 0:
                    b, s, f = "", 200000, ""
                    read_count = 0
                    while True:
                        if stop_event.is_set(): break
                        c, d = self.read_rt(b, s, f)
                        if c > 0 and d:
                            lines = d.splitlines()
                            data.extend(lines)
                            read_count += len(lines)
                        elif c <= 0: break
                    self.close_rt()
                    if read_count > 0:
                        logging.info(f"[{source_name}] << {spec} 日付一括受信成功 ({read_count}レコード分)")
                        continue
            except Exception as e:
                try: self.close_rt()
                except: pass
            
            spec_found = 0
            for jj in places:
                if stop_event.is_set(): break
                for rr in range(1, 13):
                    if stop_event.is_set(): break
                    key = f"{today_str}{jj:02d}{rr:02d}"
                    try:
                        res = self.open_rt(spec, key)
                        if res < 0: continue
                        
                        b, s, f = "", 200000, ""
                        while True:
                            if stop_event.is_set(): break
                            c, d = self.read_rt(b, s, f)
                            if c > 0 and d:
                                data.extend(d.splitlines())
                            elif c <= 0: break
                        self.close_rt()
                        spec_found += 1
                    except:
                        try: self.close_rt()
                        except: pass
            if spec_found > 0:
                logging.info(f"[{source_name}] << {spec} 個別キー受信成功 ({spec_found}レース分)")
        return data

class UmaConnFetcher:
    def __init__(self):
        try: self.nv = win32com.client.Dispatch("NVDTLabLib.NVLink")
        except: self.nv = None
        
    def init_link(self): 
        return self.nv is not None and self.nv.NVInit("UNKNOWN") == 0

    def open_rt(self, spec, key):
        res = self.nv.NVRTOpen(spec, key)
        return int(res[0] if isinstance(res, tuple) else res) if str(res[0] if isinstance(res, tuple) else res).strip() else -1

    def read_rt(self, b, s, f):
        r = self.nv.NVRead(b, s, f)
        c, d = (r[0], r[1]) if isinstance(r, tuple) else (r, "")
        try: return int(c), d
        except: return -1, ""
        
    def close_rt(self): 
        self.nv.NVClose()

    def fetch_rt_loop_uma(self, specs, today_str, source_name, stop_event: threading.Event):
        data = []
        valid_odds_keys = set()
        
        for spec in specs:
            if stop_event.is_set(): break
            logging.info(f"[{source_name}] >> {spec} の速報データを取得中...")
            
            try:
                res = self.open_rt(spec, today_str)
                if res >= 0:
                    b, s, f = "", 200000, ""
                    read_count = 0
                    while True:
                        if stop_event.is_set(): break
                        c, d = self.read_rt(b, s, f)
                        if c > 0 and d:
                            lines = d.splitlines()
                            data.extend(lines)
                            read_count += len(lines)
                            
                            if spec == "0B12":
                                for line in lines:
                                    if len(line) >= 27 and line.startswith("RA"):
                                        r_id = line[11:27]
                                        rt_key = r_id[0:8] + r_id[8:10] + r_id[14:16]
                                        if len(rt_key) == 12 and rt_key.isdigit():
                                            valid_odds_keys.add(rt_key)
                                            
                        elif c <= 0: break
                    self.close_rt()
                    if read_count > 0:
                        logging.info(f"[{source_name}] << {spec} 日付一括受信成功 ({read_count}レコード分)")
                        continue
            except Exception as e:
                logging.error(f"UmaConn read error: {e}")
                try: self.close_rt()
                except: pass

            if not valid_odds_keys:
                continue
                
            spec_found = 0
            for key in sorted(list(valid_odds_keys)):
                if stop_event.is_set(): break
                try:
                    res = self.open_rt(spec, key)
                    if res < 0: continue
                    
                    b, s, f = "", 200000, ""
                    while True:
                        if stop_event.is_set(): break
                        c, d = self.read_rt(b, s, f)
                        if c > 0 and d:
                            data.extend(d.splitlines())
                        elif c <= 0: break
                    self.close_rt()
                    spec_found += 1
                except:
                    try: self.close_rt()
                    except: pass
            if spec_found > 0:
                logging.info(f"[{source_name}] << {spec} 個別キー受信成功 ({spec_found}レース分)")

        return data