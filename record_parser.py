import logging

logger = logging.getLogger(__name__)

class JRAVanParser:
    """
    JRA-VAN の固定長レコードを解析し、辞書形式に変換するクラス。
    """
    
    def parse_o1_record(self, record_str):
        """
        O1レコード（単勝・複勝・枠連オッズ）の完全解析
        """
        try:
            record_type = record_str[0:2]
            if record_type != "O1": return None
                
            race_id = record_str[11:27]
            place_code = race_id[8:10]
            race_num = race_id[14:16]
            
            horse_count_str = record_str[35:37].strip()
            horse_count = int(horse_count_str) if horse_count_str.isdigit() else 0
            
            # --- 1. 単勝オッズ ---
            win_base, win_stride = 43, 8
            win_odds = {}
            for i in range(horse_count):
                start = win_base + (i * win_stride)
                data = record_str[start:start+win_stride]
                if len(data) < 8: break
                u_str, o_str, n_str = data[0:2].strip(), data[2:6].strip(), data[6:8].strip()
                if o_str and o_str.isdigit() and int(o_str) > 0:
                    win_odds[int(u_str) if u_str.isdigit() else i+1] = {
                        "odds": int(o_str) / 10.0, "ninki": int(n_str) if n_str.isdigit() else 99
                    }
                    
            # --- 2. 複勝オッズ ---
            show_base, show_stride = 267, 12
            show_odds = {}
            for i in range(horse_count):
                start = show_base + (i * show_stride)
                data = record_str[start:start+show_stride]
                if len(data) < 12: break
                u_str, o_min, o_max, n_str = data[0:2].strip(), data[2:6].strip(), data[6:10].strip(), data[10:12].strip()
                if o_min and o_min.isdigit() and int(o_min) > 0:
                    actual_min = int(o_min) / 10.0
                    show_odds[int(u_str) if u_str.isdigit() else i+1] = {
                        "odds_min": actual_min,
                        "odds_max": int(o_max) / 10.0 if o_max.isdigit() else actual_min,
                        "ninki": int(n_str) if n_str.isdigit() else 99
                    }
                    
            # --- 3. 枠連オッズ ---
            bracket_base, bracket_stride = 603, 8
            bracket_odds = {}
            for i in range(36):
                start = bracket_base + (i * bracket_stride)
                data = record_str[start:start+bracket_stride]
                if len(data) < 8: break
                w1, w2, o_str, n_str = data[0:1].strip(), data[1:2].strip(), data[2:6].strip(), data[6:8].strip()
                if w1 and w2 and o_str and o_str.isdigit() and int(o_str) > 0:
                    bracket_odds[f"{w1}-{w2}"] = {
                        "odds": int(o_str) / 10.0, "ninki": int(n_str) if n_str.isdigit() else 99
                    }
            
            return {
                "race_id": race_id, "place_code": place_code, "race_num": race_num,
                "win_odds": win_odds, "show_odds": show_odds, "bracket_odds": bracket_odds
            }
        except Exception as e:
            logger.error(f"O1レコード解析エラー: {e}")
            return None

    def parse_o2_record(self, record_str):
        """
        O2レコード（馬連オッズ）の解析
        ダンプデータから確定したインデックス（オフセット40, ストライド13）
        """
        try:
            if record_str[0:2] != "O2": return None
            race_id = record_str[11:27]
            
            base_offset = 40  # 修正：正確なオフセット位置
            stride = 13
            max_combos = 153
            
            quinella_odds = {}
            for i in range(max_combos):
                start = base_offset + (i * stride)
                data = record_str[start:start+stride]
                if len(data) < 13: break
                
                u1_str, u2_str, odds_str, ninki_str = data[0:2].strip(), data[2:4].strip(), data[4:10].strip(), data[10:13].strip()
                
                if u1_str and u2_str and odds_str and odds_str.isdigit() and int(odds_str) > 0:
                    quinella_odds[f"{int(u1_str)}-{int(u2_str)}"] = {
                        "odds": int(odds_str) / 10.0,
                        "ninki": int(ninki_str) if ninki_str.isdigit() else 999
                    }
                    
            return {"race_id": race_id, "quinella_odds": quinella_odds}
        except Exception as e:
            logger.error(f"O2レコード解析エラー: {e}")
            return None