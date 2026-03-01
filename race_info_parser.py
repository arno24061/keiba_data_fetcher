import logging

logger = logging.getLogger(__name__)

class RaceInfoParser:
    """
    JRA-VAN / UmaConn の生データをパースするクラス。
    推測禁止の原則に従い、バイト位置は CONFIG 辞書で厳密に管理する。
    """
    def __init__(self):
        # 【重要】マニュアルの仕様に基づくバイト位置（開始位置, 終了位置）
        # ※仕様が未確定な部分は (None, None) とし、安全のためパースをスキップする
        self.BYTE_LAYOUT_CONFIG = {
            "JRA": {
                "RA": {
                    "race_id": (11, 27),
                    # 例: 発走時刻が 30文字目〜34文字目 (HHMM) の場合 -> (30, 34)
                    # 仕様確認後、ここに正しいバイト位置を入力してください
                    "start_time_hhmm": (None, None), 
                },
                "WH": {
                    "race_id": (11, 27),
                    # 馬体重の抽出位置など
                    "weight_data": (None, None),
                }
            },
            "NAR": {
                # UmaConn用のレイアウト（JRAと異なる場合はここに定義）
                "RA": {
                    "race_id": (11, 27),
                    "start_time_hhmm": (None, None),
                }
            }
        }

    def parse_record(self, record_str: str, source: str = "JRA") -> dict:
        if not record_str or len(record_str) < 27:
            return None
            
        record_type = record_str[0:2]
        if record_type not in ["RA", "SE", "WE", "WH"]:
            return None

        source_key = "JRA" if source.lower() == "jra" else "NAR"
        layout = self.BYTE_LAYOUT_CONFIG.get(source_key, {}).get(record_type, {})
        
        # 共通のレースID抽出
        rid_start, rid_end = layout.get("race_id", (11, 27))
        race_id = record_str[rid_start:rid_end] if rid_start is not None else None
        
        parsed_data = {
            "record_type": record_type,
            "race_id": race_id,
            "raw_payload": record_str
        }

        # 発走時刻の抽出（レイアウトが設定されている場合のみ）
        st_start, st_end = layout.get("start_time_hhmm", (None, None))
        if st_start is not None and st_end is not None and len(record_str) >= st_end:
            parsed_data["start_time_hhmm"] = record_str[st_start:st_end].strip()

        return parsed_data