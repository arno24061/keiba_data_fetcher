import logging
import json
import os
import datetime

logger = logging.getLogger(__name__)

class TCSEngine:
    """
    時系列オッズ解析（TCS）エンジン。
    過去のオッズ状態をメモリおよびローカルキャッシュ(JSON)に保持し、
    プロセスの再起動や一時停止後でも過去の差分を正確に計算する。
    O1(単勝等)とO2(馬連等)を分けて独立して管理する。
    """
    def __init__(self, cache_file="tcs_history_cache.json"):
        self.cache_file = cache_file
        self.history = self._load_state()

    def _load_state(self) -> dict:
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                # 当日のデータ（YYYYMMDDから始まるキー）のみを抽出して復元
                today_str = datetime.datetime.now().strftime("%Y%m%d")
                cleaned_data = {k: v for k, v in data.items() if str(k).startswith(today_str)}
                
                if cleaned_data:
                    logger.info(f"TCS履歴キャッシュを復元しました（本日分: {len(cleaned_data)}レース）")
                return cleaned_data
            except Exception as e:
                logger.warning(f"TCSキャッシュの読み込みに失敗しました。新規で開始します: {e}")
        return {}

    def _save_state(self):
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.history, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"TCSキャッシュの保存に失敗しました: {e}")

    def calculate_tcs_features(self, race_id: str, current_odds_dict: dict, odds_type: str = "O1") -> dict:
        """
        現在のオッズ辞書を受け取り、TCS特徴量（断層、支持率変動等）を付与して返す。
        odds_type ("O1" または "O2") に応じて、キャッシュの階層と計算ロジックを分離する。
        """
        if race_id not in self.history:
            self.history[race_id] = {}
        
        # O1とO2で履歴の辞書を分離
        if odds_type not in self.history[race_id]:
            self.history[race_id][odds_type] = {}

        tcs_result = {}
        
        # --------------------------------------------------------
        # O1 (単勝ベース) の解析ロジック
        # --------------------------------------------------------
        if odds_type == "O1":
            sorted_odds = []
            for key, value in current_odds_dict.items():
                umaban_str = str(key).replace("umaban_", "")
                if not umaban_str.isdigit():
                    continue
                try:
                    odds_val = float(value["tansho"]) if isinstance(value, dict) and "tansho" in value else float(value)
                    implied_prob = 1.0 / odds_val if odds_val > 0 else 0.0
                    sorted_odds.append({"umaban": umaban_str, "odds": odds_val, "prob": implied_prob})
                except Exception:
                    continue

            sorted_odds.sort(key=lambda x: x["odds"])

            for i, item in enumerate(sorted_odds):
                umaban = item["umaban"]
                current_odds = item["odds"]
                current_prob = item["prob"]
                
                odds_gap_ratio = 1.0
                is_gap = 0
                if i > 0:
                    prev_odds = sorted_odds[i-1]["odds"]
                    if prev_odds > 0:
                        odds_gap_ratio = current_odds / prev_odds
                        if odds_gap_ratio >= 1.5:
                            is_gap = 1

                prob_diff = 0.0
                if umaban in self.history[race_id][odds_type]:
                    last_prob = self.history[race_id][odds_type][umaban]["last_prob"]
                    prob_diff = current_prob - last_prob

                tcs_result[umaban] = {
                    "latest_odds": current_odds,
                    "implied_prob": current_prob,
                    "odds_gap_ratio": round(odds_gap_ratio, 3),
                    "is_odds_gap": is_gap,
                    "prob_diff": round(prob_diff, 4)
                }

                # キャッシュ更新
                self.history[race_id][odds_type][umaban] = {
                    "last_odds": current_odds,
                    "last_prob": current_prob
                }

        # --------------------------------------------------------
        # O2 (馬連等ベース) の解析ロジック
        # --------------------------------------------------------
        elif odds_type == "O2":
            for pair, value in current_odds_dict.items():
                try:
                    odds_val = float(value["odds"]) if isinstance(value, dict) and "odds" in value else float(value)
                    
                    # 時系列でのオッズ変動差分
                    odds_diff = 0.0
                    if pair in self.history[race_id][odds_type]:
                        last_odds = self.history[race_id][odds_type][pair]["last_odds"]
                        odds_diff = odds_val - last_odds

                    tcs_result[pair] = {
                        "latest_odds": odds_val,
                        "odds_diff": round(odds_diff, 1)
                    }

                    # キャッシュ更新
                    self.history[race_id][odds_type][pair] = {
                        "last_odds": odds_val
                    }
                except Exception:
                    continue

        # 計算終了後に状態を永続化
        self._save_state()

        return tcs_result