import win32com.client
import datetime
import logging
import time
import os
import sys
import json
import warnings
import hashlib

# 32bit環境によるcryptographyのUserWarningを抑制
warnings.filterwarnings("ignore", category=UserWarning, module="cryptography")

from record_parser import JRAVanParser
from race_info_parser import RaceInfoParser
from gcs_uploader import GCSUploader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

class JRAVanFetcher:
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

    def get_today_places(self, today_str):
        places = []
        logging.info(f"[JRA-VAN] 0B01(開催スケジュール)から本日の開催場を動的抽出します...")
        try:
            res = self.open_rt("0B01", today_str)
            if res >= 0:
                b, s, f = "", 200000, ""
                while True:
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

    def fetch_rt_loop(self, specs, today_str, places, source_name):
        data = []
        for spec in specs:
            logging.info(f"[{source_name}] >> {spec} の速報データを取得中...")
            
            # 1. まずは日付キー(YYYYMMDD)での一括取得を試みる（時系列オッズやレース情報用）
            try:
                res = self.open_rt(spec, today_str)
                if res >= 0:
                    b, s, f = "", 200000, ""
                    read_count = 0
                    while True:
                        c, d = self.read_rt(b, s, f)
                        if c > 0 and d:
                            lines = d.splitlines()
                            data.extend(lines)
                            read_count += len(lines)
                        elif c <= 0: break
                    self.close_rt()
                    if read_count > 0:
                        logging.info(f"[{source_name}] << {spec} 日付一括受信成功 ({read_count}レコード分)")
                        continue # 一括で取れたら個別ループはスキップして次のspecへ
            except Exception as e:
                try: self.close_rt()
                except: pass
            
            # 2. 日付一括で取得できなかった場合（個別キー要求仕様の場合）は場×レースでフォールバック
            spec_found = 0
            for jj in places:
                for rr in range(1, 13):
                    key = f"{today_str}{jj:02d}{rr:02d}"
                    try:
                        res = self.open_rt(spec, key)
                        if res < 0: continue
                        
                        b, s, f = "", 200000, ""
                        while True:
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

    def fetch_rt_loop_uma(self, specs, today_str, source_name):
        data = []
        valid_odds_keys = set()
        
        for spec in specs:
            logging.info(f"[{source_name}] >> {spec} の速報データを取得中...")
            
            # 1. まずは日付キー(YYYYMMDD)での一括取得を試みる
            try:
                res = self.open_rt(spec, today_str)
                if res >= 0:
                    b, s, f = "", 200000, ""
                    read_count = 0
                    while True:
                        c, d = self.read_rt(b, s, f)
                        if c > 0 and d:
                            lines = d.splitlines()
                            data.extend(lines)
                            read_count += len(lines)
                            
                            # 0B12(レース詳細)を受信した際、フォールバック通信用のキー(12桁)を生成しておく
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
                        continue # 成功したら個別キー取得はスキップして次のspecへ
            except Exception as e:
                logging.error(f"UmaConn read error: {e}")
                try: self.close_rt()
                except: pass

            # 2. 日付キーで取得できなかった場合（オッズ系が個別キーを要求する場合）、抽出したキーでフォールバック
            if not valid_odds_keys:
                continue
                
            spec_found = 0
            for key in sorted(list(valid_odds_keys)):
                try:
                    res = self.open_rt(spec, key)
                    if res < 0: continue
                    
                    b, s, f = "", 200000, ""
                    while True:
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
        
        # 発表月日時分(MMDDHHMM)を時系列のキーにするのは、オッズレコードのみに限定
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

if __name__ == "__main__":
    print("=== 統合データフェッチャー (時系列オッズ0B41仕様・フェイルセーフ対応版) 起動 ===")
    
    jra = JRAVanFetcher()
    uma = UmaConnFetcher()
    odds_parser = JRAVanParser()
    info_parser = RaceInfoParser()
    uploader = GCSUploader()
    upload_cache = UploadCache()
    
    jra_ready = jra.init_link()
    uma_ready = uma.init_link()
    
    if not jra_ready and not uma_ready:
        logging.error("通信初期化に失敗しました。システムを終了します。")
        exit(1)
        
    current_interval = 300
    # 0B41, 0B42（時系列オッズ履歴）と、0B31, 0B32（最新速報オッズ）を両方指定し、
    # 過去の履歴も最新のスナップショットも確実に取りこぼしなく取得する
    specs_to_fetch = ["0B12", "0B15", "0B11", "0B41", "0B42", "0B31", "0B32"]
    
    while True:
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        logging.info(f"--- 定期取得サイクル 開始 (設定インターバル: {current_interval}秒) ---")
        all_results = {}
        
        if jra_ready:
            jra_places = jra.get_today_places(today_str)
            jra_data = jra.fetch_rt_loop(specs_to_fetch, today_str, jra_places, "JRA-VAN")
            jra_res = process_and_upload(jra_data, odds_parser, info_parser, uploader, "jra", upload_cache)
            all_results.update(jra_res)
                
        if uma_ready:
            uma_data = uma.fetch_rt_loop_uma(specs_to_fetch, today_str, "UmaConn")
            uma_res = process_and_upload(uma_data, odds_parser, info_parser, uploader, "nar", upload_cache)
            all_results.update(uma_res)
                
        current_interval = determine_poll_interval(all_results)
        
        logging.info(f"--- サイクル完了。{current_interval}秒待機します ---")
        time.sleep(current_interval)