import logging

logger = logging.getLogger(__name__)

class JRAVanParser:
    """
    JRA-VAN の固定長レコードを解析し、辞書形式に変換するクラス。
    ダンプデータから事実確認された正確なインデックスを使用。
    """
    
    def parse_o1_record(self, record_str):
        """
        O1レコード（単勝・複勝・枠連オッズ）の解析
        """
        try:
            # レコード種別 (0-1)
            record_type = record_str[0:2]
            if record_type != "O1":
                return None
                
            # レースID (インデックス 11〜26 の16文字)
            race_id = record_str[11:27]
            place_code = race_id[8:10]
            race_num = race_id[14:16]
            
            # 出走頭数 (インデックス 35〜36)
            horse_count_str = record_str[35:37].strip()
            horse_count = int(horse_count_str) if horse_count_str.isdigit() else 0
            
            # 単勝オッズデータはインデックス 43 から開始
            # 1頭あたり 8文字 (馬番2 + オッズ4 + 人気2)
            base_offset = 43
            horse_stride = 8
            
            odds_dict = {}
            for i in range(horse_count):
                start = base_offset + (i * horse_stride)
                end = start + horse_stride
                
                horse_data = record_str[start:end]
                if len(horse_data) < 8:
                    break
                    
                umaban_str = horse_data[0:2].strip()
                odds_str = horse_data[2:6].strip()
                ninki_str = horse_data[6:8].strip()
                
                if odds_str and odds_str.isdigit() and int(odds_str) > 0:
                    umaban = int(umaban_str) if umaban_str.isdigit() else i + 1
                    # オッズは10倍値なので10で割る (例: 0055 -> 5.5)
                    actual_odds = int(odds_str) / 10.0
                    ninki = int(ninki_str) if ninki_str.isdigit() else 99
                    
                    odds_dict[umaban] = {
                        "odds": actual_odds,
                        "ninki": ninki
                    }
            
            return {
                "race_id": race_id,
                "place_code": place_code,
                "race_num": race_num,
                "win_odds": odds_dict
            }
            
        except Exception as e:
            logger.error(f"O1レコード解析エラー: {e}")
            return None