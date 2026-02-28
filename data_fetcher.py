import win32com.client
import datetime
import logging
from record_parser import JRAVanParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class JRAVanFetcher:
    """中央競馬(JRA-VAN)用フェッチャー"""
    def __init__(self):
        try:
            self.jv = win32com.client.Dispatch("JVDTLab.JVLink")
        except Exception:
            self.jv = None
            
    def init_link(self):
        return self.jv is not None and self.jv.JVInit("UNKNOWN") == 0

    def fetch_realtime_odds(self):
        if not self.jv: return []
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        all_data = []
        logging.info(f"[JRA-VAN] 中央オッズ走査中... ({today_str})")
        
        for spec in ["0B31", "0B32"]:
            for jj in range(1, 11): # 中央の場コードは01〜10
                for rr in range(1, 13):
                    key = f"{today_str}{jj:02d}{rr:02d}"
                    try:
                        res = self.jv.JVRTOpen(spec, key)
                        r_code = int(res[0] if isinstance(res, tuple) else res) if str(res[0] if isinstance(res, tuple) else res).strip() else 0
                        if r_code < 0: continue
                            
                        buff, size, fname = "", 200000, ""
                        while True:
                            read_res = self.jv.JVRead(buff, size, fname)
                            c, d = (read_res[0], read_res[1]) if isinstance(read_res, tuple) else (read_res, "")
                            try: c = int(c)
                            except: break
                            
                            if c > 0:
                                if d: all_data.append(d)
                            elif c == 0: break
                            elif c == -1: continue
                            else: break
                        self.jv.JVClose()
                    except Exception:
                        try: self.jv.JVClose()
                        except: pass
        return all_data

class UmaConnFetcher:
    """地方競馬(UmaConn)用フェッチャー"""
    def __init__(self):
        try:
            self.nv = win32com.client.Dispatch("NVDTLabLib.NVLink")
        except Exception as e:
            logging.error(f"UmaConn オブジェクト生成失敗: {e}")
            self.nv = None
            
    def init_link(self):
        if not self.nv: return False
        if self.nv.NVInit("UNKNOWN") != 0: return False
        logging.info("UmaConn (NV-Link) 初期化成功")
        return True

    def fetch_realtime_odds(self):
        if not self.nv: return []
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        all_data = []
        logging.info(f"[UmaConn] 地方オッズ走査中... ({today_str})")
        
        # 地方は O1, O2 の仕様IDも中央と同一
        for spec in ["0B31", "0B32"]:
            # 地方の場コードは広範(帯広03〜佐賀55等)なため、1〜59を走査
            for jj in range(1, 60):
                for rr in range(1, 13):
                    key = f"{today_str}{jj:02d}{rr:02d}"
                    try:
                        # 地方は NVRTOpen メソッドを使用する
                        res = self.nv.NVRTOpen(spec, key)
                        r_code = int(res[0] if isinstance(res, tuple) else res) if str(res[0] if isinstance(res, tuple) else res).strip() else 0
                        if r_code < 0: continue
                            
                        buff, size, fname = "", 200000, ""
                        while True:
                            # 地方は NVRead メソッドを使用する
                            read_res = self.nv.NVRead(buff, size, fname)
                            c, d = (read_res[0], read_res[1]) if isinstance(read_res, tuple) else (read_res, "")
                            try: c = int(c)
                            except: break
                            
                            if c > 0:
                                if d: all_data.append(d)
                            elif c == 0: break
                            elif c == -1: continue
                            else: break
                        self.nv.NVClose()
                    except Exception:
                        try: self.nv.NVClose()
                        except: pass
        return all_data

if __name__ == "__main__":
    print("=== 地方競馬(UmaConn) パース・テスト開始 ===")
    uma_fetcher = UmaConnFetcher()
    parser = JRAVanParser() # 中央と同一のパーサーを使用
    
    if uma_fetcher.init_link():
        raw_data = uma_fetcher.fetch_realtime_odds()
        logging.info(f"取得した地方競馬の有効データ: {len(raw_data)}件")
        
        parsed_o1, parsed_o2 = 0, 0
        
        for record_str in raw_data:
            if record_str.startswith("O1") and parsed_o1 == 0:
                data = parser.parse_o1_record(record_str)
                if data:
                    print(f"\n🏁 地方レースID: {data['race_id']} 【単勝(先頭2頭)】")
                    for u, info in list(data['win_odds'].items())[:2]: print(f"  {u:2d}番 : {info['odds']:5.1f}倍")
                    parsed_o1 += 1
            elif record_str.startswith("O2") and parsed_o2 == 0:
                data = parser.parse_o2_record(record_str)
                if data:
                    print(f"\n🏁 地方レースID: {data['race_id']} 【馬連(先頭3組)】")
                    for combo, info in list(data['quinella_odds'].items())[:3]: print(f"  {combo} : {info['odds']:5.1f}倍")
                    parsed_o2 += 1
                    
            if parsed_o1 > 0 and parsed_o2 > 0: break
                
    print("\n=== テスト終了 ===")