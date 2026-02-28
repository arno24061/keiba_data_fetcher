import win32com.client
import datetime
import logging
from record_parser import JRAVanParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class JRAVanFetcher:
    """
    JRA-VAN (JV-Link) と通信し、メモリ上にデータを取得するクラス。
    """
    def __init__(self):
        try:
            self.jv = win32com.client.Dispatch("JVDTLab.JVLink")
        except Exception as e:
            logging.error(f"JV-Link オブジェクト生成失敗: {e}")
            self.jv = None
            
    def init_link(self):
        if not self.jv: return False
        ret = self.jv.JVInit("UNKNOWN")
        if ret != 0:
            logging.error(f"JRA-VAN 初期化エラー (コード: {ret})")
            return False
        logging.info("JRA-VAN (JV-Link) 初期化成功")
        return True

    def fetch_realtime_odds(self):
        if not self.jv: return []
        
        data_spec = "0B31"
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        all_odds_data = []
        
        logging.info(f"本日の全レース(最大120R)のオッズ取得走査を開始します... ({today_str})")
        
        for jj in range(1, 11):
            for rr in range(1, 13):
                key = f"{today_str}{jj:02d}{rr:02d}"
                
                try:
                    result = self.jv.JVRTOpen(data_spec, key)
                    
                    ret_val = result[0] if isinstance(result, tuple) else result
                    try:
                        ret_code = int(ret_val) if str(ret_val).strip() else 0
                    except ValueError:
                        ret_code = -1
                    
                    if ret_code < 0:
                        continue
                        
                    buff = ""
                    size = 200000
                    filename = ""
                    
                    while True:
                        read_result = self.jv.JVRead(buff, size, filename)
                        
                        if isinstance(read_result, tuple):
                            read_code = read_result[0]
                            data_str = read_result[1]
                        else:
                            read_code = read_result
                            data_str = ""
                            
                        try:
                            read_code = int(read_code)
                        except (ValueError, TypeError):
                            break
                            
                        if read_code > 0:
                            if data_str:
                                all_odds_data.append(data_str)
                        elif read_code == 0:
                            break
                        elif read_code == -1:
                            continue
                        else:
                            break
                            
                    self.jv.JVClose()
                    
                except Exception as e:
                    logging.error(f"キー {key} の通信処理で例外エラー: {e}")
                    try:
                        self.jv.JVClose()
                    except:
                        pass
                        
        return all_odds_data

if __name__ == "__main__":
    print("=== オッズパース・テスト開始 ===")
    
    jra_fetcher = JRAVanFetcher()
    parser = JRAVanParser()
    
    if jra_fetcher.init_link():
        raw_data = jra_fetcher.fetch_realtime_odds()
        
        logging.info(f"取得した有効な生データ(レコード)総数: {len(raw_data)}件")
        
        if len(raw_data) > 0:
            for record_str in raw_data:
                # O1レコード（単勝オッズ）のみを処理対象とする
                if record_str.startswith("O1"):
                    parsed_data = parser.parse_o1_record(record_str)
                    if parsed_data:
                        r_id = parsed_data['race_id']
                        logging.info(f"🏁 レースID: {r_id} の単勝オッズを解析しました")
                        
                        for umaban, info in parsed_data['win_odds'].items():
                            print(f"  馬番 {umaban:2d} : {info['odds']:5.1f}倍 ({info['ninki']}番人気)")
                        
                        print("-" * 30)
                        # テスト用：最初の1レース分を綺麗に表示したら終了
                        break
        else:
            logging.info("データがありませんでした。")
            
    print("=== テスト終了 ===")