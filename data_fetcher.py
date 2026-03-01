import win32com.client
import datetime
import logging
import time
import os
import json
import warnings
import hashlib

# 32bit環境によるcryptographyのUserWarningを抑制
warnings.filterwarnings("ignore", category=UserWarning, module="cryptography")

from record_parser import JRAVanParser
from race_info_parser import RaceInfoParser
from gcs_uploader import GCSUploader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class UploadCache:
    """
    GCSへの無駄な重複アップロードを防ぐための状態管理クラス。
    一度アップロードしたパスや、ペイロードのハッシュ値をローカルに記憶する。
    """
    def __init__(self, cache_file="upload_cache.json"):
        self.cache_file = cache_file
        self.cache = self._load()

    def _load(self):
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return set(json.load(f))
            except Exception:
                pass
        return set()

    def _save(self):
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(list(self.cache), f)
        except Exception as e:
            logging.error(f"Upload cache save error: {e}")

    def is_uploaded(self, cache_key: str) -> bool:
        return cache_key in self.cache

    def mark_as_uploaded(self, cache_key: str):
        self.cache.add(cache_key)
        self._save()

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
                    except:
                        try: self.close_rt()
                        except: pass
            if spec_found > 0:
                logging.info(f"[{source_name}] << {spec} 受信成功 ({spec_found}件)")
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
        data = []
        for spec in specs:
            logging.info(f"[{source_name}] >> {spec} の最新速報を走査中...")
            try:
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
                    logging.info(f"[{source_name}] << {spec} 受信成功 ({read_count}件)")
            except Exception as e:
                logging.error(f"UmaConn read error: {e}")
                try: self.close_rt()
                except: pass
        return data

def process_and_upload(raw_data, odds_parser, info_parser, uploader, source_prefix, upload_cache):
    if not raw_data:
        return {}

    merged_data = {}
    timestamp = datetime.datetime.now().isoformat()
    today_str = datetime.datetime.now().strftime("%Y%m%d")
    
    for record_str in raw_data:
        if len(record_str) < 35:
            continue

        record_type = record_str[0:2].upper()
        
        # 発表月日時分(MMDDHHMM)を直接スライスし、時系列のキー(ファイル名)にする
        happyo_time = record_str[27:35]
        if not happyo_time.isdigit():
            happyo_time = "latest"

        parsed = None
        if record_type in ["RA", "SE", "WE", "WH"]:
            parsed = info_parser.parse_record(record_str, source=source_prefix)
        elif record_type == "O1":
            parsed = odds_parser.parse_o1_record(record_str)
        elif record_type == "O2":
            parsed = odds_parser.parse_o2_record(record_str)

        # パース失敗時（地方O2など）は生文字列を強制保存し、Streamlitのパーサーに委ねる
        if not parsed:
            r_id_raw = record_str[11:27]
            if r_id_raw.isdigit():
                parsed = {
                    "race_id": r_id_raw,
                    "record_type": record_type,
                    "raw_payload": record_str
                }

        if parsed and "race_id" in parsed:
            r_id = parsed["race_id"]
            r_type = parsed.get("record_type", record_type)

            if r_id not in merged_data:
                merged_data[r_id] = {}
            
            # 発表時刻ごとの辞書を作成
            if happyo_time not in merged_data[r_id]:
                merged_data[r_id][happyo_time] = {
                    "race_id": r_id,
                    "fetched_at": timestamp,
                    "happyo_time": happyo_time,
                    "source": source_prefix,
                    "records": {}
                }

            if r_type not in merged_data[r_id][happyo_time]["records"]:
                merged_data[r_id][happyo_time]["records"][r_type] = []

            merged_data[r_id][happyo_time]["records"][r_type].append(parsed)

    upload_count = 0
    skip_count = 0
    
    # レースごと、発表時刻ごとに別々のファイルとしてGCSへ保存
    for r_id, time_dict in merged_data.items():
        for h_time, data_dict in time_dict.items():
            blob_name = f"odds_history/{source_prefix}/{today_str}/{r_id}/{h_time}.json"
            
            # --- 重複チェックと差分検知 ---
            if h_time != "latest":
                # 定刻オッズは一度アップロードすれば不変なので、パス自体をキャッシュキーにする
                cache_key = blob_name
            else:
                # latestの場合は中身が変わる可能性があるため、ペイロードのハッシュ値をキーにする
                dict_str = json.dumps(data_dict, sort_keys=True)
                content_hash = hashlib.md5(dict_str.encode('utf-8')).hexdigest()
                cache_key = f"{blob_name}_{content_hash}"

            if upload_cache.is_uploaded(cache_key):
                skip_count += 1
                continue

            if uploader.upload_json(blob_name, data_dict):
                upload_cache.mark_as_uploaded(cache_key)
                upload_count += 1
            
    if upload_count > 0 or skip_count > 0:
        logging.info(f"[{source_prefix}] GCS保存状況: 新規 {upload_count}件 / 重複スキップ {skip_count}件")
    
    return merged_data

def determine_poll_interval(all_merged_data: dict) -> int:
    now = datetime.datetime.now()
    imminent_race_found = False
    
    for r_id, data in all_merged_data.items():
        for h_time, time_data in data.items():
            for r_type, records in time_data.get("records", {}).items():
                for rec in records:
                    st_hhmm = rec.get("start_time_hhmm")
                    if st_hhmm and isinstance(st_hhmm, str) and st_hhmm.isdigit() and len(st_hhmm) == 4:
                        try:
                            start_dt = now.replace(hour=int(st_hhmm[:2]), minute=int(st_hhmm[2:]), second=0, microsecond=0)
                            diff_seconds = (start_dt - now).total_seconds()
                            if -300 <= diff_seconds <= 900:
                                imminent_race_found = True
                                break
                        except Exception: continue
                if imminent_race_found: break
            if imminent_race_found: break
        if imminent_race_found: break
            
    if imminent_race_found:
        logging.warning("⚠️ 発走15分以内のレースを検知しました。可変インターバル(30秒)に移行します。")
        return 30
    return 300

if __name__ == "__main__":
    print("=== 統合データフェッチャー (重複排除・最適化版) 起動 ===")
    
    jra = JRAVanFetcher()
    uma = UmaConnFetcher()
    odds_parser = JRAVanParser()
    info_parser = RaceInfoParser()
    uploader = GCSUploader()
    upload_cache = UploadCache()  # 重複排除用キャッシュの初期化
    
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
            jra_res = process_and_upload(jra_data, odds_parser, info_parser, uploader, "jra", upload_cache)
            all_results.update(jra_res)
                
        if uma_ready:
            uma_data = uma._fetch_rt_loop_uma(specs_to_fetch, today_str, "UmaConn")
            uma_res = process_and_upload(uma_data, odds_parser, info_parser, uploader, "nar", upload_cache)
            all_results.update(uma_res)
                
        current_interval = determine_poll_interval(all_results)
        
        logging.info(f"--- サイクル完了。{current_interval}秒待機します ---")
        time.sleep(current_interval)