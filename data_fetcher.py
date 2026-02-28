import win32com.client
import datetime
import logging

# ログ出力の設定（画面に経過を表示するため）
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class JRAVanFetcher:
    """
    JRA-VAN (JV-Link) と直接通信し、メモリ上にデータを取得するクラス。
    ディスクI/Oを発生させず、メインマシンに負荷をかけない設計。
    """
    def __init__(self):
        try:
            self.jv = win32com.client.Dispatch("JVDTLab.JVLink")
        except Exception as e:
            logging.error(f"JV-Link オブジェクト生成失敗: {e}")
            self.jv = None
            
    def init_link(self):
        if not self.jv: return False
        
        # 認証初期化 ("UNKNOWN" は JV-Link 側で設定された標準キーを使用する指定)
        ret = self.jv.JVInit("UNKNOWN")
        if ret != 0:
            logging.error(f"JRA-VAN 初期化エラー (コード: {ret})")
            return False
        logging.info("JRA-VAN (JV-Link) 初期化成功")
        return True

    def fetch_realtime_odds(self):
        if not self.jv: return []
        
        # データ種別 0B14 = 速報オッズ
        # Option 1 = 最新の蓄積データのみを取得
        data_spec = "0B14"
        # 取得開始時間を「昨日」に設定し、現在までの最新データを要求する
        from_time = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d000000")
        
        # COMのoutパラメータ用ダミー変数
        read_count = 0
        download_count = 0
        last_timestamp = ""
        
        logging.info("JRA-VAN サーバーへ速報オッズ(0B14)を要求中...")
        
        try:
            # Pythonのwin32comでは、outパラメータはタプルとして返却される
            result = self.jv.JVOpen(data_spec, from_time, 1, read_count, download_count, last_timestamp)
            
            if isinstance(result, tuple):
                ret_code = result[0]
            else:
                ret_code = result
                
            if ret_code < 0:
                logging.error(f"JVOpen エラー (コード: {ret_code}) - データが存在しないか、通信失敗")
                self.jv.JVClose()
                return []
                
            logging.info(f"JVOpen 成功 (コード: {ret_code})。ストリーム読み込みを開始します。")
            
            data_list = []
            buff_size = 100000  # 1回の読み取りバッファサイズ
            filename = ""
            
            while True:
                read_result = self.jv.JVRead(buff_size, filename)
                
                if isinstance(read_result, tuple):
                    read_code = read_result[0]
                    data_str = read_result[1]
                else:
                    break
                    
                if read_code == 0:
                    break  # EOF (全データ読み込み完了)
                elif read_code == -1:
                    continue  # ファイルの切り替わり（スキップして継続）
                elif read_code < 0:
                    logging.error(f"JVRead エラー (コード: {read_code})")
                    break
                    
                if data_str:
                    # JRA-VANから送られてくる固定長のShift-JIS文字列をリストに格納
                    data_list.append(data_str)
                    
            self.jv.JVClose()
            return data_list
            
        except Exception as e:
            logging.error(f"JRA-VAN 通信中の例外エラー: {e}")
            self.jv.JVClose()
            return []


class UmaConnFetcher:
    """
    地方競馬DATA (UmaConn) と通信するクラス。
    基本仕様はJRA-VAN (JV-Link) と同一インターフェースを持つ。
    """
    def __init__(self):
        try:
            self.nv = win32com.client.Dispatch("NVDTLabLib.NVLink")
        except Exception as e:
            logging.error(f"UmaConn オブジェクト生成失敗: {e}")
            self.nv = None
            
    def init_link(self):
        if not self.nv: return False
        ret = self.nv.NVInit("UNKNOWN")
        if ret != 0:
            logging.error(f"UmaConn 初期化エラー (コード: {ret})")
            return False
        logging.info("UmaConn (NV-Link) 初期化成功")
        return True


if __name__ == "__main__":
    print("=== データフェッチ・テスト開始 ===")
    
    # JRA-VANからのオッズ取得テスト
    jra_fetcher = JRAVanFetcher()
    if jra_fetcher.init_link():
        odds_data = jra_fetcher.fetch_realtime_odds()
        logging.info(f"取得したJRAオッズの生データ件数: {len(odds_data)} 件")
        
        if len(odds_data) > 0:
            logging.info("▼ データサンプル (先頭1件の100文字)")
            logging.info(odds_data[0][:100])
        else:
            logging.info("現在取得できる最新のオッズデータはありませんでした（本日のレースが終了している、または未配信）")
            
    print("=== テスト終了 ===")