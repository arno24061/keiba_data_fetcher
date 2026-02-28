import win32com.client
import datetime
import logging
import time
import json
from record_parser import JRAVanParser
from gcs_uploader import GCSUploader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class JRAVanFetcher:
    def __init__(self):
        try: self.jv = win32com.client.Dispatch("JVDTLab.JVLink")
        except: self.jv = None
    def init_link(self):
        return self.jv is not None and self.jv.JVInit("UNKNOWN") == 0
    def fetch_realtime_odds(self):
        if not self.jv: return []
        t_str, data = datetime.datetime.now().strftime("%Y%m%d"), []
        logging.info(f"[JRA-VAN] 中央オッズ走査中... ({t_str})")
        for spec in ["0B31", "0B32"]:
            for jj in range(1, 11):
                for rr in range(1, 13):
                    try:
                        res = self.jv.JVRTOpen(spec, f"{t_str}{jj:02d}{rr:02d}")
                        r_code = int(res[0] if isinstance(res, tuple) else res) if str(res[0] if isinstance(res, tuple) else res).strip() else 0
                        if r_code < 0: continue
                        b, s, f = "", 200000, ""
                        while True:
                            r = self.jv.JVRead(b, s, f)
                            c, d = (r[0], r[1]) if isinstance(r, tuple) else (r, "")
                            try: c = int(c)
                            except: break
                            if c > 0:
                                if d: data.append(d)
                            elif c == 0: break
                            elif c == -1: continue
                            else: break
                        self.jv.JVClose()
                    except:
                        try: self.jv.JVClose()
                        except: pass
        return data

class UmaConnFetcher:
    def __init__(self):
        try: self.nv = win32com.client.Dispatch("NVDTLabLib.NVLink")
        except: self.nv = None
    def init_link(self):
        return self.nv is not None and self.nv.NVInit("UNKNOWN") == 0
    def fetch_realtime_odds(self):
        if not self.nv: return []
        t_str, data = datetime.datetime.now().strftime("%Y%m%d"), []
        logging.info(f"[UmaConn] 地方オッズ走査中... ({t_str})")
        for spec in ["0B31", "0B32"]:
            for jj in range(1, 60):
                for rr in range(1, 13):
                    try:
                        res = self.nv.NVRTOpen(spec, f"{t_str}{jj:02d}{rr:02d}")
                        r_code = int(res[0] if isinstance(res, tuple) else res) if str(res[0] if isinstance(res, tuple) else res).strip() else 0
                        if r_code < 0: continue
                        b, s, f = "", 200000, ""
                        while True:
                            r = self.nv.NVRead(b, s, f)
                            c, d = (r[0], r[1]) if isinstance(r, tuple) else (r, "")
                            try: c = int(c)
                            except: break
                            if c > 0:
                                if d: data.append(d)
                            elif c == 0: break
                            elif c == -1: continue
                            else: break
                        self.nv.NVClose()
                    except:
                        try: self.nv.NVClose()
                        except: pass
        return data

def process_and_upload(raw_data, parser, uploader, source_prefix):
    """
    取得した生データをパースし、レースID単位で合体させてGCSへアップロードする関数。
    """
    if not raw_data:
        logging.info(f"[{source_prefix}] アップロード対象データなし")
        return

    merged_data = {}
    timestamp = datetime.datetime.now().isoformat()
    today_str = datetime.datetime.now().strftime("%Y%m%d")
    
    for record_str in raw_data:
        parsed_data = None
        if record_str.startswith("O1"):
            parsed_data = parser.parse_o1_record(record_str)
        elif record_str.startswith("O2"):
            parsed_data = parser.parse_o2_record(record_str)
            
        if parsed_data:
            r_id = parsed_data["race_id"]
            if r_id not in merged_data:
                # 辞書の初期化。基本情報をセット。
                merged_data[r_id] = {
                    "race_id": r_id,
                    "fetched_at": timestamp,
                    "source": source_prefix,
                    "place_code": parsed_data.get("place_code", ""),
                    "race_num": parsed_data.get("race_num", "")
                }
            # O1データ(単勝/複勝/枠連)とO2データ(馬連)をマージする
            for key, value in parsed_data.items():
                if key not in ["race_id", "place_code", "race_num"]:
                    merged_data[r_id][key] = value

    upload_count = 0
    for r_id, data_dict in merged_data.items():
        blob_name = f"odds/{source_prefix}/{today_str}/{r_id}.json"
        if uploader.upload_json(blob_name, data_dict):
            upload_count += 1
            
    logging.info(f"[{source_prefix}] GCSアップロード完了: {upload_count}レース分")


if __name__ == "__main__":
    print("=== オッズ自動取得・GCS常駐保存システム 起動 ===")
    
    # 1. 各モジュールのインスタンス化
    jra = JRAVanFetcher()
    uma = UmaConnFetcher()
    parser = JRAVanParser()
    uploader = GCSUploader()
    
    # 2. 接続初期化 (通信ポートの確保)
    jra_ready = jra.init_link()
    uma_ready = uma.init_link()
    
    if not jra_ready and not uma_ready:
        logging.error("中央・地方ともに通信初期化に失敗しました。システムを終了します。")
        exit(1)
    
    # ポーリング間隔（秒）
    INTERVAL_SECONDS = 300 # 5分
    
    while True:
        logging.info("--- 定期取得サイクル 開始 ---")
        
        # 3. 中央(JRA)の処理フロー
        if jra_ready:
            try:
                jra_raw_data = jra.fetch_realtime_odds()
                process_and_upload(jra_raw_data, parser, uploader, "jra")
            except Exception as e:
                logging.error(f"JRA処理ループ内で例外発生: {e}")
                
        # 4. 地方(UmaConn)の処理フロー
        if uma_ready:
            try:
                uma_raw_data = uma.fetch_realtime_odds()
                process_and_upload(uma_raw_data, parser, uploader, "nar")
            except Exception as e:
                logging.error(f"UmaConn処理ループ内で例外発生: {e}")
                
        # 5. インターバル待機
        logging.info(f"--- サイクル完了。{INTERVAL_SECONDS}秒間待機します ---")
        time.sleep(INTERVAL_SECONDS)