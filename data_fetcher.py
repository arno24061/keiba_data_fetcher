import os
import time
import datetime
import logging
import threading
import warnings
from PIL import Image, ImageDraw
import pystray
from pystray import MenuItem as item

# 32bit環境によるcryptographyのUserWarningを抑制
warnings.filterwarnings("ignore", category=UserWarning, module="cryptography")

from record_parser import JRAVanParser
from race_info_parser import RaceInfoParser
from gcs_uploader import GCSUploader

# 分割したモジュールの読み込み
from fetchers import JRAVanFetcher, UmaConnFetcher
from processor import UploadCache, process_and_upload, determine_poll_interval, get_base_dir

# ==========================================
# ロガー設定 & スレッド制御オブジェクト
# ==========================================
log_file = os.path.join(get_base_dir(), 'fetcher.log')
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file, encoding='utf-8')]
)

# グローバル変数の代わりに、スレッド間で安全に状態を共有・制御できる Event を使用
stop_event = threading.Event()

# ==========================================
# 常駐用スレッド処理
# ==========================================
def data_fetch_loop():
    logging.info("=== 統合データフェッチャー (バックグラウンド常駐版) 起動 ===")
    
    jra = JRAVanFetcher()
    uma = UmaConnFetcher()
    odds_parser = JRAVanParser()
    info_parser = RaceInfoParser()
    uploader = GCSUploader()
    upload_cache = UploadCache()
    
    jra_ready = jra.init_link()
    uma_ready = uma.init_link()
    
    if not jra_ready and not uma_ready:
        logging.error("通信初期化に失敗しました。取得スレッドを終了します。")
        return
        
    current_interval = 300
    specs_to_fetch = ["0B12", "0B15", "0B11", "0B41", "0B42", "0B31", "0B32"]
    
    while not stop_event.is_set():
        try:
            today_str = datetime.datetime.now().strftime("%Y%m%d")
            logging.info(f"--- 定期取得サイクル 開始 (設定インターバル: {current_interval}秒) ---")
            all_results = {}
            
            if jra_ready and not stop_event.is_set():
                jra_places = jra.get_today_places(today_str, stop_event)
                jra_data = jra.fetch_rt_loop(specs_to_fetch, today_str, jra_places, "JRA-VAN", stop_event)
                jra_res = process_and_upload(jra_data, odds_parser, info_parser, uploader, "jra", upload_cache)
                all_results.update(jra_res)
                    
            if uma_ready and not stop_event.is_set():
                uma_data = uma.fetch_rt_loop_uma(specs_to_fetch, today_str, "UmaConn", stop_event)
                uma_res = process_and_upload(uma_data, odds_parser, info_parser, uploader, "nar", upload_cache)
                all_results.update(uma_res)
                    
            current_interval = determine_poll_interval(all_results)
            logging.info(f"--- サイクル完了。{current_interval}秒待機します ---")
            
            # 停止要求が来た場合に即抜けられるよう、1秒刻みで待機
            for _ in range(current_interval):
                if stop_event.is_set():
                    break
                time.sleep(1)

        except Exception as e:
            logging.error(f"❌ ループ内で予期せぬエラー: {e}", exc_info=True)
            for _ in range(10):
                if stop_event.is_set(): break
                time.sleep(1)

    logging.info("🛑 取得スレッドが安全に停止しました。")

# ==========================================
# タスクトレイ UI処理
# ==========================================
def create_image():
    image = Image.new('RGB', (64, 64), color=(0, 100, 0))
    dc = ImageDraw.Draw(image)
    dc.rectangle((16, 16, 48, 48), fill=(255, 255, 255))
    return image

def on_quit(icon, item):
    logging.info("ユーザー操作により終了処理を開始します...")
    stop_event.set() # イベントを発火させ、取得スレッドを停止
    icon.stop()

def main():
    fetch_thread = threading.Thread(target=data_fetch_loop, daemon=True)
    fetch_thread.start()

    image = create_image()
    menu = pystray.Menu(
        item('Keiba Fetcher 稼働中', lambda: None, enabled=False),
        item('終了 (Quit)', on_quit)
    )
    
    icon = pystray.Icon("KeibaFetcher", image, "Keiba Data Fetcher", menu)
    icon.run()

if __name__ == "__main__":
    main()