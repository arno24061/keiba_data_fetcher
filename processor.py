import os
import sys
import json
import hashlib
import datetime
import logging

def get_base_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

class UploadCache:
    def __init__(self, cache_filename="upload_cache.json"):
        self.cache_file = os.path.join(get_base_dir(), cache_filename)
        self.cache = self._load()

    def _load(self):
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return set(json.load(f))
            except Exception as e:
                logging.warning(f"キャッシュの読み込みに失敗しました。新規作成します: {e}")
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
        
        if record_type in ["O1", "O2", "O3", "O4", "O5", "O6"]:
            happyo_time = record_str[27:35]
            if not happyo_time.isdigit():
                happyo_time = "latest"
        else:
            happyo_time = "latest"

        parsed = None
        if record_type in ["RA", "SE", "WE", "WH"]:
            parsed = info_parser.parse_record(record_str, source=source_prefix)
        elif record_type in ["O1", "O2", "O3", "O4", "O5", "O6"]:
            if record_type == "O1":
                parsed = odds_parser.parse_o1_record(record_str)
            elif record_type == "O2":
                parsed = odds_parser.parse_o2_record(record_str)
            else:
                r_id_raw = record_str[11:27]
                parsed = {"race_id": r_id_raw, "record_type": record_type, "raw_payload": record_str}

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

    upload_tasks = []
    skip_count = 0
    
    for r_id, time_dict in merged_data.items():
        for h_time, data_dict in time_dict.items():
            blob_name = f"odds_history/{source_prefix}/{today_str}/{r_id}/{h_time}.json"
            
            if h_time != "latest":
                cache_key = blob_name
            else:
                dict_str = json.dumps(data_dict, sort_keys=True)
                content_hash = hashlib.md5(dict_str.encode('utf-8')).hexdigest()
                cache_key = f"{blob_name}_{content_hash}"

            if upload_cache.is_uploaded(cache_key):
                skip_count += 1
                continue

            upload_tasks.append((blob_name, data_dict, cache_key))

    upload_count = 0
    if upload_tasks:
        tasks_for_uploader = [(task[0], task[1]) for task in upload_tasks]
        successful_blobs = uploader.upload_jsons_parallel(tasks_for_uploader)
        
        upload_count = len(successful_blobs)
        success_set = set(successful_blobs)
        
        for blob_name, _, cache_key in upload_tasks:
            if blob_name in success_set:
                upload_cache.mark_as_uploaded(cache_key)
            
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