import win32com.client
import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class JVLinkDiagnostic:
    """
    JRA-VANの疎通確認および「0件問題」の事実特定を行うための診断クラス。
    指定されたデータ種別に対し全場コードを走査し、実際の応答コードと生データをダンプします。
    """
    def __init__(self):
        try:
            self.jv = win32com.client.Dispatch("JVDTLab.JVLink")
        except Exception as e:
            logging.error(f"JVLinkの初期化に失敗しました: {e}")
            self.jv = None

    def init_link(self):
        if self.jv is None:
            return False
        return self.jv.JVInit("UNKNOWN") == 0

    def run_diagnostic(self, target_date: str):
        """
        指定された日付に対して、1〜59の全場コードを走査し、各データ種別の応答を確認する。
        """
        if not self.jv:
            return

        # 0B01(スケジュール), 0B12(出馬表), 0B15(馬場), 0B11(馬体重) を検証
        specs = ["0B01", "0B14", "0B12", "0B15", "0B11"]
        logging.info(f"--- 診断開始 (対象日: {target_date}) ---")

        for spec in specs:
            logging.info(f"[{spec}] の走査を開始します...")
            found_any = False
            
            for jj in range(1, 60):
                # レース番号は1で固定し、その場の開催有無をチェック
                for rr in range(1, 13):
                    key = f"{target_date}{jj:02d}{rr:02d}"
                    try:
                        res = self.jv.JVRTOpen(spec, key)
                        r_code = int(res[0] if isinstance(res, tuple) else res) if str(res[0] if isinstance(res, tuple) else res).strip() else -1
                        
                        if r_code >= 0:
                            logging.info(f"★ 取得成功: {spec} / キー: {key} (応答コード: {r_code})")
                            found_any = True
                            
                            # データを1件読み込んで中身を確認
                            b, s, f = "", 200000, ""
                            r = self.jv.JVRead(b, s, f)
                            c, d = (r[0], r[1]) if isinstance(r, tuple) else (r, "")
                            try:
                                c = int(c)
                            except ValueError:
                                c = -1
                                
                            if c > 0 and d:
                                print(f"\n▼ 事実確認用ダンプ ({spec} - キー:{key}) ▼")
                                print("-" * 60)
                                print(d[:150])
                                print("-" * 60)
                                print("▲ コピーをお願いします ▲\n")
                            
                            self.jv.JVClose()
                            break # その場でのデータ存在が確認できたため、次の場へ移行
                        else:
                            try:
                                self.jv.JVClose()
                            except Exception:
                                pass
                    except Exception as e:
                        logging.error(f"走査中の例外 ({key}): {e}")
                        try:
                            self.jv.JVClose()
                        except:
                            pass
                            
                # 0B01等のマスタ系データは場コードに依存しない場合があるため、
                # 1件でも見つかれば次のデータ種別検証へ移る
                if found_any:
                    break
            
            if not found_any:
                logging.warning(f"[{spec}] 本日有効なデータキーが1件も見つかりませんでした。")

if __name__ == "__main__":
    print("=== JRA-VAN 疎通確認・事実特定ツール 起動 ===")
    diagnostic = JVLinkDiagnostic()
    if diagnostic.init_link():
        # 本日の日付でテストを実行
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        diagnostic.run_diagnostic(today_str)
    else:
        logging.error("JV-Linkの接続に失敗しました。")
    print("=== 診断終了 ===")