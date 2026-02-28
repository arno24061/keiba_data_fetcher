import win32com.client

def test_com_objects():
    print("--- COMオブジェクト起動テスト開始 ---")
    
    # 1. JRA-VAN (JV-Link) のテスト
    try:
        jv_link = win32com.client.Dispatch("JVDTLab.JVLink")
        print("✅ JRA-VAN (JV-Link) の起動・接続に成功しました。")
    except Exception as e:
        print(f"❌ JRA-VAN 起動エラー: {e}")
        print("※JRA-VAN Data Lab対応ソフト（JV-Link）がインストールされているか確認してください。")

    # 2. UmaConn (NV-Link) のテスト
    try:
        # 地方競馬データ（UmaConn）のモジュール呼び出し
        nv_link = win32com.client.Dispatch("NVDTLabLib.NVLink")
        print("✅ UmaConn (NV-Link) の起動・接続に成功しました。")
    except Exception as e:
        print(f"❌ UmaConn 起動エラー: {e}")
        print("※UmaConnが正しくインストール・設定されているか確認してください。")

    print("--- テスト終了 ---")

if __name__ == "__main__":
    test_com_objects()