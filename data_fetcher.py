import win32com.client
import datetime
import logging
import time
import os
import json
import warnings

# 32bit環境によるcryptographyのUserWarningを抑制
warnings.filterwarnings("ignore", category=UserWarning, module="cryptography")

from record_parser import JRAVanParser
from race_info_parser import RaceInfoParser
from tcs_engine import TCSEngine
from gcs_uploader import GCSUploader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BaseFetcher:
    def _fetch_rt_loop(self, specs, today_str, max_places, source_name):
        data = []
        for spec in specs:
            logging.info(f"[{source_name}] >> {spec} の最新速報を走査中...")
            spec_found = 0
            for jj in range(1, max_places + 1):
                for rr in range(1, 13):
                    key = f"{today_str}{jj:02d}{rr:02d}"
                    try:
                        res = self.open_rt(spec, key)
                        if res < 0: continue
                        
                        b, s, f = "", 200000, ""
                        while True:
                            c, d = self.read_rt(b, s, f)
                            if c > 0 and d:
                                data.append(d)
                            elif c <= 0: break
                        self.close_rt()
                        spec_found += 1
                        logging.info(f"[{source_name}] ★ 取得成功: {spec} / {key}")
                    except:
                        try: self.close_rt()
                        except: pass
            if spec_found > 0:
                logging.info(f"[{source_name}] << {spec} 最新データの取得成功 ({spec_found}件)")
        return data

class JRAVanFetcher(BaseFetcher):
    def __init__(self):
        try: self.jv = win32com.client.Dispatch("JVDTLab.JVLink")
        except: self.jv = None
    def init_link(self): return self.jv is not None and self.jv.JVInit("UNKNOWN") == 0

    def open_rt(self, spec, key):
        res = self.jv.JVRTOpen(spec, key)
        return int(res[0] if isinstance(res, tuple) else res) if str(res[0] if isinstance(res, tuple) else res).strip() else -1
    def read_rt(self, b, s, f):
        r = self.jv.JVRead(b, s, f)
        c, d = (r[0], r[1]) if isinstance(r, tuple) else (r, "")
        try: return int(c), d
        except: return -1, ""
    def close_rt(self): self.jv.JVClose()

class UmaConnFetcher(BaseFetcher):
    def __init__(self):
        try: self.nv = win32com.client.Dispatch("NVDTLabLib.NVLink")
        except: self.nv = None
    def init_link(self): return self.nv is not None and self.nv.NVInit("UNKNOWN") == 0

    def open_rt(self, spec, key):
        res = self.nv.NVRTOpen(spec, key)
        return int(res[0] if isinstance(res, tuple) else res) if str(res[0] if isinstance(res, tuple) else res).strip() else -1
    def read_rt(self, b, s, f):
        r = self.nv.NVRead(b, s, f)
        c, d = (r[0], r[1]) if isinstance(r, tuple) else (r, "")
        try: return int(c), d
        except: return -1, ""
    def close_rt(self): self.nv.NVClose()

    def _fetch_rt_loop_uma(self, specs, today_str, source_name):
        """
        地方競馬(UmaConn)専用のリアルタイム走査ループ。
        場コードやレース番号を細かく指定せず、本日の日付だけで一括要求する仕様に適合。
        """
        data = []
        for spec in specs:
            logging.info(f"[{source_name}] >> {spec} の最新速報を走査中...")
            try:
                # NVRTOpenは、日付(YYYYMMDD)のみをキーにすることで本日分の全データを取得可能
                res = self.open_rt(spec, today_str)
                if res < 0:
                    continue
                
                b, s, f = "", 200000, ""
                read_count = 0
                while True:
                    c, d = self.read_rt(b, s, f)
                    if c > 0 and d:
                        data.append(d)
                        read_count += 1
                    elif c <= 0: break
                self.close_rt()
                if read_count > 0:
                    logging.info(f"[{source_name}] ★ 取得成功: {spec} ({read_count}件)")
            except Exception as e:
                logging.error(f"UmaConn read error: {e}")
                try: self.close_rt()
                except: pass
        return data

def process_and_upload(raw_data, odds_parser, info_parser, tcs_engine, uploader, source_prefix):
    if not raw_data:
        return {}

    merged_data = {}
    timestamp = datetime.datetime.now().isoformat()
    today_str = datetime.datetime.now().strftime("%Y%m%d")
    
    for record_str in raw_data:
        record_type = record_str[0:2]
        
        if record_type in ["O1", "O2"]:
            parsed = odds_parser.parse_o1_record(record_str) if record_type == "O1" else odds_parser.parse_o2_record(record_str)
            if parsed and "race_id" in parsed:
                r_id = parsed["race_id"]
                if r_id not in merged_data:
                    merged_data[r_id] = {"race_id": r_id, "fetched_at": timestamp, "source": source_prefix, "race_info": {}}
                
                if record_type == "O2":
                    o2_odds = parsed.get("win_odds", parsed.get("odds", {}))
                    if o2_odds:
                        tcs_engine.calculate_tcs_features(r_id, o2_odds, odds_type="O2")
                elif record_type == "O1":
                    current_odds = parsed.get("win_odds", parsed.get("odds", {}))
                    if current_odds:
                        tcs_features = tcs_engine.calculate_tcs_features(r_id, current_odds, odds_type="O1")
                        merged_data[r_id]["tcs_features"] = tcs_features

                for k, v in parsed.items():
                    if k != "race_id": 
                        merged_data[r_id][k] = v
                        
        elif record_type in ["RA", "SE", "WE", "WH"]:
            parsed = info_parser.parse_record(record_str, source=source_prefix)
            
            # パーサーがNoneを返した場合でも、共通バイト位置(12〜27)からIDを抜き出して強制保存する安全装置
            if not parsed:
                if len(record_str) >= 27:
                    r_id_raw = record_str[11:27]
                    if r_id_raw.isdigit():
                        parsed = {
                            "race_id": r_id_raw,
                            "record_type": record_type,
                            "raw_payload": record_str
                        }

            if parsed:
                r_id = parsed["race_id"]
                r_type = parsed["record_type"]
                payload = parsed["raw_payload"]
                
                if r_id not in merged_data:
                    merged_data[r_id] = {"race_id": r_id, "fetched_at": timestamp, "source": source_prefix, "race_info": {}}
                
                if "start_time_hhmm" in parsed:
                    merged_data[r_id]["start_time_hhmm"] = parsed["start_time_hhmm"]
                    
                if r_type == "SE":
                    if "SE" not in merged_data[r_id]["race_info"]: merged_data[r_id]["race_info"]["SE"] = []
                    merged_data[r_id]["race_info"]["SE"].append(payload)
                else:
                    merged_data[r_id]["race_info"][r_type] = payload

    upload_count = 0
    for r_id, data_dict in merged_data.items():
        blob_name = f"odds/{source_prefix}/{today_str}/{r_id}.json"
        if uploader.upload_json(blob_name, data_dict):
            upload_count += 1
            
    if upload_count > 0:
        logging.info(f"[{source_prefix}] GCSアップロード完了: {upload_count}レース (TCS状態同期済)")
    return merged_data

def determine_poll_interval(all_merged_data: dict) -> int:
    now = datetime.datetime.now()
    imminent_race_found = False
    
    for r_id, data in all_merged_data.items():
        st_hhmm = data.get("start_time_hhmm")
        if not st_hhmm or not st_hhmm.isdigit() or len(st_hhmm) != 4: continue
        try:
            start_dt = now.replace(hour=int(st_hhmm[:2]), minute=int(st_hhmm[2:]), second=0, microsecond=0)
            diff_seconds = (start_dt - now).total_seconds()
            if -300 <= diff_seconds <= 900:
                imminent_race_found = True
                break
        except Exception: continue
            
    if imminent_race_found:
        logging.warning("⚠️ 発走15分以内のレースを検知しました。可変インターバル(30秒)に移行します。")
        return 30
    return 300

if __name__ == "__main__":
    print("=== 統合データフェッチャー (安定稼働・UmaConn対応版) 起動 ===")
    
    jra = JRAVanFetcher()
    uma = UmaConnFetcher()
    odds_parser = JRAVanParser()
    info_parser = RaceInfoParser()
    tcs_engine = TCSEngine()
    uploader = GCSUploader()
    
    jra_ready = jra.init_link()
    uma_ready = uma.init_link()
    
    if not jra_ready and not uma_ready:
        logging.error("通信初期化に失敗しました。システムを終了します。")
        exit(1)
        
    current_interval = 300
    specs_to_fetch = ["0B12", "0B15", "0B11", "0B31", "0B32"]
    
    while True:
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        logging.info(f"--- 定期取得サイクル 開始 (設定インターバル: {current_interval}秒) ---")
        all_results = {}
        
        if jra_ready:
            jra_data = jra._fetch_rt_loop(specs_to_fetch, today_str, 10, "JRA-VAN")
            jra_res = process_and_upload(jra_data, odds_parser, info_parser, tcs_engine, uploader, "jra")
            all_results.update(jra_res)
                
        if uma_ready:
            # UmaConnは専用の取得ループを使用する
            uma_data = uma._fetch_rt_loop_uma(specs_to_fetch, today_str, "UmaConn")
            uma_res = process_and_upload(uma_data, odds_parser, info_parser, tcs_engine, uploader, "nar")
            all_results.update(uma_res)
                
        current_interval = determine_poll_interval(all_results)
        
        logging.info(f"--- サイクル完了。{current_interval}秒待機します ---")
        time.sleep(current_interval)