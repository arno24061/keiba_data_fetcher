import os
import time
import datetime
import logging
import threading
import warnings
import queue
import tkinter as tk
from tkinter import scrolledtext
from PIL import Image, ImageDraw
import pystray
from pystray import MenuItem as item
import pythoncom

# 32bit環境によるcryptographyのUserWarningを抑制
warnings.filterwarnings("ignore", category=UserWarning, module="cryptography")

from record_parser import JRAVanParser
from race_info_parser import RaceInfoParser
from gcs_uploader import GCSUploader

from fetchers import JRAVanFetcher, UmaConnFetcher
from processor import UploadCache, process_and_upload, extract_race_schedule, get_base_dir

# ==========================================
# ロガー設定 & スレッド間通信用キュー
# ==========================================
stop_event = threading.Event()
log_queue = queue.Queue()

class QueueLogHandler(logging.Handler):
    def emit(self, record):
        log_queue.put(self.format(record))

log_file = os.path.join(get_base_dir(), 'fetcher.log')
formatter = logging.Formatter('%(asctime)s - [%(name)s] - %(levelname)s - %(message)s')

file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setFormatter(formatter)

queue_handler = QueueLogHandler()
queue_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(file_handler)
logger.addHandler(queue_handler)

# ==========================================
# 並行ワーカー関数 (JRA/NAR独立)
# ==========================================
def fetch_worker_loop(source_name, fetcher_class, odds_parser, info_parser, uploader, upload_cache):
    pythoncom.CoInitialize()
    fetcher = None
    
    try:
        fetcher = fetcher_class()
        
        if not fetcher.init_link():
            logging.error(f"[{source_name}] 通信初期化に失敗しました。スレッドを終了します。")
            return

        schedule = {}
        last_full_sync = 0
        FULL_SYNC_INTERVAL = 300  # 5分 (全体同期および閑散期の基本待機)
        SHORT_SYNC_INTERVAL = 60  # 60秒 (対象レース検知時の待機)
        
        full_specs = ["0B12", "0B15", "0B11", "0B41", "0B42", "0B31", "0B32"]
        odds_specs = ["0B41", "0B42", "0B31", "0B32"]
        
        logging.info(f"[{source_name}] --- ワーカー稼働開始 ---")

        while not stop_event.is_set():
            current_time = time.time()
            today_str = datetime.datetime.now().strftime("%Y%m%d")
            
            # --- 1. 全体同期サイクル (前回同期から5分以上経過時のみ) ---
            if current_time - last_full_sync >= FULL_SYNC_INTERVAL:
                logging.info(f"[{source_name}] 🔄 --- 全体同期サイクル開始 ---")
                if source_name == "JRA-VAN":
                    places = fetcher.get_today_places(today_str, stop_event)
                    if places:
                        raw_data = fetcher.fetch_rt_loop(full_specs, today_str, places, source_name, stop_event)
                    else:
                        raw_data = []
                else:
                    raw_data = fetcher.fetch_rt_loop_uma(full_specs, today_str, source_name, stop_event)
                    
                res = process_and_upload(raw_data, odds_parser, info_parser, uploader, "jra" if source_name=="JRA-VAN" else "nar", upload_cache)
                schedule.update(extract_race_schedule(res))
                
                # 同期が完了したら時刻を更新
                last_full_sync = time.time()
                logging.info(f"[{source_name}] 🔄 --- 全体同期完了 ---")

            # --- 2. ピンポイント同期サイクル & インターバル判定 ---
            now_dt = datetime.datetime.now()
            imminent_keys = []
            
            for key, start_dt in schedule.items():
                diff_sec = (start_dt - now_dt).total_seconds()
                # 発送15分前(900秒) 〜 発送後10分(-600秒) までを対象とする
                if -600 <= diff_sec <= 900: 
                    imminent_keys.append(key)
                    
            if imminent_keys and not stop_event.is_set():
                logging.info(f"[{source_name}] 🎯 発送直前レース検知 ({len(imminent_keys)}件): {imminent_keys}")
                raw_data = fetcher.fetch_specific_races(odds_specs, imminent_keys, source_name, stop_event)
                process_and_upload(raw_data, odds_parser, info_parser, uploader, "jra" if source_name=="JRA-VAN" else "nar", upload_cache)
                
                # 直前レースがある場合は待機時間を1分(60秒)に短縮
                current_interval = SHORT_SYNC_INTERVAL
            else:
                # 直前レースがない（または非開催）場合は待機時間を5分(300秒)に設定
                current_interval = FULL_SYNC_INTERVAL

            # --- 3. 次のチェックまで待機 ---
            logging.info(f"[{source_name}] 次のサイクルまで {current_interval}秒 待機します...")
            for _ in range(current_interval):
                if stop_event.is_set(): break
                time.sleep(1)

    except Exception as e:
        logging.error(f"[{source_name}] ループ内エラー: {e}", exc_info=True)

    finally:
        logging.info(f"[{source_name}] 🛑 COMオブジェクトのメモリ解放処理を実行中...")
        if fetcher:
            fetcher.cleanup()
        pythoncom.CoUninitialize()
        logging.info(f"[{source_name}] 🛑 ワーカーが安全に停止しました。")


# ==========================================
# GUI & タスクトレイ UI処理
# ==========================================
class FetcherGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Keiba Data Fetcher - 実行ログ")
        self.root.geometry("850x450")
        self.root.protocol("WM_DELETE_WINDOW", self.hide_window)

        self.text_area = scrolledtext.ScrolledText(
            self.root, state='disabled', 
            bg='#1e1e1e', fg='#d4d4d4', font=('Consolas', 10)
        )
        self.text_area.pack(expand=True, fill='both', padx=5, pady=5)

        self.root.withdraw()
        self.update_log_widget()

    def update_log_widget(self):
        has_new_logs = False
        while not log_queue.empty():
            try:
                msg = log_queue.get_nowait()
                self.text_area.config(state='normal')
                self.text_area.insert(tk.END, msg + "\n")
                has_new_logs = True
            except queue.Empty:
                break
        
        if has_new_logs:
            self.text_area.see(tk.END)
            self.text_area.config(state='disabled')
            
        self.root.after(200, self.update_log_widget)

    def show_window(self):
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()

    def hide_window(self):
        self.root.withdraw()

    def quit_app(self):
        self.root.quit()

def create_image():
    image = Image.new('RGB', (64, 64), color=(0, 100, 0))
    dc = ImageDraw.Draw(image)
    dc.rectangle((16, 16, 48, 48), fill=(255, 255, 255))
    return image

def start_tray_icon(app_instance):
    def on_quit(icon, item):
        logging.info("ユーザー操作により終了処理を開始します...")
        stop_event.set()
        icon.stop()
        app_instance.root.after(0, app_instance.quit_app)

    def on_show(icon, item):
        app_instance.root.after(0, app_instance.show_window)

    image = create_image()
    menu = pystray.Menu(
        item('ログを表示', on_show, default=True),
        item('終了 (Quit)', on_quit)
    )
    
    icon = pystray.Icon("KeibaFetcher", image, "Keiba Data Fetcher", menu)
    icon.run()

def main():
    logging.info("=== 統合データフェッチャー (並列＆ピンポイント常駐版) 起動 ===")
    app = FetcherGUI()

    odds_parser = JRAVanParser()
    info_parser = RaceInfoParser()
    uploader = GCSUploader()
    upload_cache = UploadCache()

    jra_thread = threading.Thread(
        target=fetch_worker_loop, 
        args=("JRA-VAN", JRAVanFetcher, odds_parser, info_parser, uploader, upload_cache), 
        daemon=False
    )
    uma_thread = threading.Thread(
        target=fetch_worker_loop, 
        args=("UmaConn", UmaConnFetcher, odds_parser, info_parser, uploader, upload_cache), 
        daemon=False
    )
    
    jra_thread.start()
    uma_thread.start()

    tray_thread = threading.Thread(target=start_tray_icon, args=(app,), daemon=True)
    tray_thread.start()

    app.root.mainloop()

if __name__ == "__main__":
    main()