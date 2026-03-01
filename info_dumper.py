import win32com.client
import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ForceDumper:
    def __init__(self):
        try: self.jv = win32com.client.Dispatch("JVDTLab.JVLink")
        except: self.jv = None

    def init_link(self):
        return self.jv is not None and self.jv.JVInit("UNKNOWN") == 0

    def force_dump(self):
        if not self.jv: return
        today = datetime.datetime.now().strftime("%Y%m%d")
        # 0B12は全レースの基本情報
        spec = "0B12"
        
        logging.info(f"--- 強制走査開始 (本日: {today}) ---")
        
        for jj in range(1, 60):
            for rr in range(1, 13):
                key = f"{today}{jj:02d}{rr:02d}"
                res = self.jv.JVRTOpen(spec, key)
                r_code = int(res[0] if isinstance(res, tuple) else res) if str(res[0] if isinstance(res, tuple) else res).strip() else -1
                
                if r_code == 0:
                    print(f"発見: {key} (データ取得成功)")
                    # 最初の1レコードだけ中身を確認
                    b, s, f = "", 200000, ""
                    r = self.jv.JVRead(b, s, f)
                    c, d = (r[0], r[1]) if isinstance(r, tuple) else (r, "")
                    if d: print(f"  -> 内容: {d[:50]}")
                    self.jv.JVClose()
                else:
                    # 0以外（エラー）は無視
                    if self.jv: self.jv.JVClose()

if __name__ == "__main__":
    dumper = ForceDumper()
    if dumper.init_link():
        dumper.force_dump()
    print("=== 強制走査終了 ===")