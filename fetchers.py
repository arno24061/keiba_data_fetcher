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

    def cleanup(self):
        try:
            self.close_rt()
        except:
            pass
        self.jv = None

    def get_today_places(self, today_str, stop_event: threading.Event):
        places = set()
        res = self.open_rt("0B15", today_str)
        
        if res == -1:
            logging.info("[JRA-VAN] 0B15(出馬表)が存在しません。本日はJRAの非開催日です。")
            return []
        elif res < -1:
            logging.warning(f"[JRA-VAN] 開催場取得エラー(レスポンス: {res})。フェイルセーフとして総当たり(1-10)を実行します。")
            return list(range(1, 11))

        b, s, f = "", 200000, ""
        while not stop_event.is_set():
            c, d = self.read_rt(b, s, f)
            if c > 0 and d:
                for line in d.splitlines():
                    if len(line) >= 21 and line.startswith("RA"):
                        place_code = line[19:21]
                        if place_code.isdigit():
                            places.add(int(place_code))
            elif c <= 0: break
        self.close_rt()
            
        places_list = sorted(list(places))
        logging.info(f"[JRA-VAN] 本日の開催場コードを特定: {places_list}")
        return places_list

    def fetch_rt_loop(self, specs, today_str, places, source_name, stop_event: threading.Event):
        data = []
        for spec in specs:
            if stop_event.is_set(): break
            
            logging.info(f"[{source_name}] >> {spec} の速報データを取得中...")
            res = self.open_rt(spec, today_str)
            if res >= 0:
                b, s, f = "", 200000, ""
                read_count = 0
                while not stop_event.is_set():
                    c, d = self.read_rt(b, s, f)
                    if c > 0 and d:
                        lines = d.splitlines()
                        data.extend(lines)
                        read_count += len(lines)
                    elif c <= 0: break
                self.close_rt()
                logging.info(f"[{source_name}] << {spec} 日付一括受信完了 ({read_count}レコード分)")
                continue 
            
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
                        while not stop_event.is_set():
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
                logging.info(f"[{source_name}] << {spec} 個別キー受信完了 ({spec_found}レース分)")
        return data

    def fetch_specific_races(self, specs, keys, source_name, stop_event: threading.Event):
        data = []
        for spec in specs:
            if stop_event.is_set(): break
            spec_found = 0
            for key in keys:
                if stop_event.is_set(): break
                res = self.open_rt(spec, key)
                if res < 0: continue
                b, s, f = "", 200000, ""
                while not stop_event.is_set():
                    c, d = self.read_rt(b, s, f)
                    if c > 0 and d:
                        data.extend(d.splitlines())
                    elif c <= 0: break
                self.close_rt()
                spec_found += 1
            if spec_found > 0:
                logging.info(f"[{source_name}] 🎯 << {spec} ピンポイント受信完了 ({spec_found}レース分)")
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

    def cleanup(self):
        try:
            self.close_rt()
        except:
            pass
        self.nv = None

    def fetch_rt_loop_uma(self, specs, today_str, source_name, stop_event: threading.Event):
        data = []
        valid_odds_keys = set()
        
        for spec in specs:
            if stop_event.is_set(): break
            
            logging.info(f"[{source_name}] >> {spec} の速報データを取得中...")
            res = self.open_rt(spec, today_str)
            if res >= 0:
                b, s, f = "", 200000, ""
                read_count = 0
                while not stop_event.is_set():
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
                logging.info(f"[{source_name}] << {spec} 日付一括受信完了 ({read_count}レコード分)")
                continue

            if not valid_odds_keys:
                continue
                
            spec_found = 0
            for key in sorted(list(valid_odds_keys)):
                if stop_event.is_set(): break
                try:
                    res = self.open_rt(spec, key)
                    if res < 0: continue
                    b, s, f = "", 200000, ""
                    while not stop_event.is_set():
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
                logging.info(f"[{source_name}] << {spec} 個別キー受信完了 ({spec_found}レース分)")
        return data

    def fetch_specific_races(self, specs, keys, source_name, stop_event: threading.Event):
        data = []
        for spec in specs:
            if stop_event.is_set(): break
            spec_found = 0
            for key in keys:
                if stop_event.is_set(): break
                res = self.open_rt(spec, key)
                if res < 0: continue
                b, s, f = "", 200000, ""
                while not stop_event.is_set():
                    c, d = self.read_rt(b, s, f)
                    if c > 0 and d:
                        data.extend(d.splitlines())
                    elif c <= 0: break
                self.close_rt()
                spec_found += 1
            if spec_found > 0:
                logging.info(f"[{source_name}] 🎯 << {spec} ピンポイント受信完了 ({spec_found}レース分)")
        return data