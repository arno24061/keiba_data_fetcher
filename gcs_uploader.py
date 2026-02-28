import os
import json
import logging
from google.cloud import storage

logger = logging.getLogger(__name__)

class GCSUploader:
    """
    パース済みのデータをGoogle Cloud StorageにJSONとして直接アップロードするクラス。
    """
    def __init__(self, bucket_name="keiba-analysis-keiba-data"):
        # 環境変数 GCS_BUCKET_NAME があれば優先し、なければ引数の keiba-analysis-keiba-data を使用
        self.bucket_name = os.environ.get("GCS_BUCKET_NAME", bucket_name)
        try:
            self.client = storage.Client()
            self.bucket = self.client.bucket(self.bucket_name)
            logger.info(f"GCSクライアント初期化成功: ターゲットバケット [{self.bucket_name}]")
        except Exception as e:
            logger.error(f"GCSクライアント初期化エラー (認証情報の確認が必要です): {e}")
            self.client = None
            self.bucket = None

    def upload_json(self, destination_blob_name, data_dict):
        """
        辞書データをJSON文字列に変換し、GCSへ直接アップロードする。
        """
        if not self.bucket:
            return False
            
        try:
            blob = self.bucket.blob(destination_blob_name)
            # 日本語が文字化けしないよう ensure_ascii=False を指定
            json_data = json.dumps(data_dict, ensure_ascii=False)
            blob.upload_from_string(json_data, content_type='application/json')
            logger.info(f"GCS保存完了: gs://{self.bucket_name}/{destination_blob_name}")
            return True
        except Exception as e:
            logger.error(f"GCSアップロード失敗 ({destination_blob_name}): {e}")
            return False