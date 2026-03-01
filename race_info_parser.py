import logging

logger = logging.getLogger(__name__)

class RaceInfoParser:
    """
    JRA-VAN / UmaConn の生データをパースし、構造化された辞書に変換するクラス。
    JVData仕様書（Ver.4.9.0.1）に基づき、Shift-JISのバイト配列として厳密に切り出しを行う。
    """
    def __init__(self):
        # JVData仕様書に基づくバイト位置（開始位置[0始まり], 終了位置, データ型）
        self.JRA_LAYOUT = {
            "RA": { # ２．レース詳細 (レコード長: 1272)
                "race_id": (11, 27, "str"),           # 位置12, 16バイト
                "distance": (697, 701, "int"),        # 位置698, 4バイト
                "track_type": (705, 707, "str"),      # 位置706, 2バイト (コード表2009)
                "course_div": (709, 711, "str"),      # 位置710, 2バイト
                "start_time_hhmm": (873, 877, "str"), # 位置874, 4バイト
                "weather_code": (887, 888, "str"),    # 位置888, 1バイト
                "turf_condition": (888, 889, "str"),  # 位置889, 1バイト
                "dirt_condition": (889, 890, "str"),  # 位置890, 1バイト
                "race_name_abbr": (572, 592, "str"),  # 位置573, 20バイト (競走名略称10文字)
            },
            "SE": { # ３．馬毎レース情報 (レコード長: 555)
                "race_id": (11, 27, "str"),           # 位置12, 16バイト
                "wakuban": (27, 28, "int"),           # 位置28, 1バイト
                "umaban": (28, 30, "int"),            # 位置29, 2バイト
                "blood_id": (30, 40, "str"),          # 位置31, 10バイト
                "horse_name": (40, 76, "str"),        # 位置41, 36バイト
                "sex_code": (78, 79, "str"),          # 位置79, 1バイト
                "age": (82, 84, "int"),               # 位置83, 2バイト
                "jockey_name": (306, 314, "str"),     # 位置307, 8バイト
                "weight": (324, 327, "int"),          # 位置325, 3バイト
                "weight_diff": (328, 331, "str"),     # 位置329, 3バイト (増減差)
                "win_odds": (359, 363, "str"),        # 位置360, 4バイト (※9999=999.9倍。計算前生データ)
                "win_ninki": (363, 365, "int"),       # 位置364, 2バイト
            },
            "WE": { # １０２．天候馬場状態 (レコード長: 42)
                "race_id": (11, 27, "str"),           # 位置12, 16バイト
                "weather_code": (34, 35, "str"),      # 位置35, 1バイト
                "turf_condition": (35, 36, "str"),    # 位置36, 1バイト
                "dirt_condition": (36, 37, "str"),    # 位置37, 1バイト
            }
        }
        
        self.BYTE_LAYOUT_CONFIG = {
            "JRA": self.JRA_LAYOUT,
            "NAR": self.JRA_LAYOUT
        }

    def _extract_value(self, record_bytes: bytes, start: int, end: int, data_type: str):
        """バイト配列から指定範囲を切り出し、Shift-JISデコードと型変換を行う"""
        if start is None or end is None or len(record_bytes) < end:
            return None
        
        try:
            # Shift-JISでデコードし、前後の空白を除去
            raw_val = record_bytes[start:end].decode('shift_jis', errors='replace').strip()
        except Exception:
            return None

        # 全角スペースや完全な空白のみの場合は欠損値(None)として扱う
        if not raw_val or raw_val.isspace() or raw_val.replace('　', '').strip() == '':
            return None
            
        try:
            if data_type == "int":
                return int(raw_val)
            elif data_type == "float":
                return float(raw_val)
            else:
                return raw_val
        except ValueError:
            logger.debug(f"型変換エラー: '{raw_val}' を {data_type} に変換できませんでした。文字列として保持します。")
            return raw_val 

    def _parse_wh_record(self, record_bytes: bytes, source_key: str) -> dict:
        """馬体重 (WH) レコードの特殊パース処理（バイトベース対応版）"""
        race_id = self._extract_value(record_bytes, 11, 27, "str")
        if not race_id:
            return None
            
        parsed_data = {
            "record_type": "WH",
            "source": source_key,
            "race_id": race_id,
            "horse_weights": []
        }
        
        base_offset = 35 
        stride = 45      
        max_horses = 18
        
        for i in range(max_horses):
            offset = base_offset + (i * stride)
            if len(record_bytes) < offset + stride:
                break
                
            horse_data_bytes = record_bytes[offset:offset+stride]
            
            umaban = self._extract_value(horse_data_bytes, 0, 2, "int") 
            if not umaban:
                continue 
                
            weight_data = {
                "umaban": umaban,
                "horse_name": self._extract_value(horse_data_bytes, 2, 38, "str"),    
                "weight": self._extract_value(horse_data_bytes, 38, 41, "int"),       
                "weight_sign": self._extract_value(horse_data_bytes, 41, 42, "str"),  
                "weight_diff": self._extract_value(horse_data_bytes, 42, 45, "str"),  
            }
            parsed_data["horse_weights"].append(weight_data)
            
        return parsed_data

    def parse_record(self, record_str: str, source: str = "JRA") -> dict:
        if not record_str or len(record_str) < 27:
            return None
            
        record_type = record_str[0:2].upper()
        if record_type not in ["RA", "SE", "WE", "WH"]:
            return None

        source_key = "JRA" if source.lower() == "jra" else "NAR"
        
        # Pythonの文字列を一度Shift-JISのバイト配列に変換する（文字数とバイト数のズレを防止）
        try:
            record_bytes = record_str.encode('shift_jis', errors='replace')
        except Exception as e:
            logger.error(f"エンコードエラー: {e}")
            return None
        
        if record_type == "WH":
            return self._parse_wh_record(record_bytes, source_key)
        
        source_config = self.BYTE_LAYOUT_CONFIG.get(source_key, {})
        layout = source_config.get(record_type, {})
        
        parsed_data = {
            "record_type": record_type,
            "source": source_key,
        }

        for field_name, (start, end, dtype) in layout.items():
            if start is not None and end is not None:
                val = self._extract_value(record_bytes, start, end, dtype)
                if val is not None:
                    parsed_data[field_name] = val

        if "race_id" not in parsed_data:
            return None

        return parsed_data