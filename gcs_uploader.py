import os
import json
import logging
from google.cloud import storage
import concurrent.futures

logger = logging.getLogger(__name__)

class GCSUploader:
    """
    パース済みのデータをGoogle Cloud StorageにJSONとして直接アップロードするクラス。
    """
    def __init__(self, bucket_name="keiba-analysis-keiba-data", max_workers=10):
        self.bucket_name = os.environ.get("GCS_BUCKET_NAME", bucket_name)
        self.max_workers = max_workers
        try:
            self.client = storage.Client()
            self.bucket = self.client.bucket(self.bucket_name)
            logger.info(f"GCSクライアント初期化成功: ターゲットバケット [{self.bucket_name}] (最大並列数: {self.max_workers})")
        except Exception as e:
            logger.error(f"GCSクライアント初期化エラー (認証情報の確認が必要です): {e}")
            self.client = None
            self.bucket = None

    def _upload_single(self, destination_blob_name, data_dict):
        """内部用の単一ファイルアップロード処理"""
        if not self.bucket:
            return False, destination_blob_name
            
        try:
            blob = self.bucket.blob(destination_blob_name)
            # 日本語が文字化けしないよう ensure_ascii=False を指定
            json_data = json.dumps(data_dict, ensure_ascii=False)
            blob.upload_from_string(json_data, content_type='application/json')
            return True, destination_blob_name
        except Exception as e:
            logger.error(f"GCSアップロード失敗 ({destination_blob_name}): {e}")
            return False, destination_blob_name

    def upload_json(self, destination_blob_name, data_dict):
        """
        辞書データをJSON文字列に変換し、GCSへ直接アップロードする（同期版）。
        """
        success, _ = self._upload_single(destination_blob_name, data_dict)
        if success:
            logger.info(f"GCS保存完了: gs://{self.bucket_name}/{destination_blob_name}")
        return success

    def upload_jsons_parallel(self, upload_tasks):
        """
        複数のJSONをスレッドプールを用いて並列アップロードする。
        upload_tasks: [(destination_blob_name, data_dict), ...]
        戻り値: アップロードに成功した destination_blob_name のリスト
        """
        if not self.bucket or not upload_tasks:
            return []

        successful_blobs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # タスクをスレッドプールに投入
            future_to_blob = {
                executor.submit(self._upload_single, blob_name, data): blob_name
                for blob_name, data in upload_tasks
            }
            
            # 完了したものから結果を回収
            for future in concurrent.futures.as_completed(future_to_blob):
                success, blob_name = future.result()
                if success:
                    successful_blobs.append(blob_name)
                    
        if successful_blobs:
            logger.info(f"GCS並列保存完了: 一括で {len(successful_blobs)} 件のファイルをアップロードしました")
            
        return successful_blobs