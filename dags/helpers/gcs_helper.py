from pandas import DataFrame
from typing import NoReturn, union
from google.cloud import storage


class GoogleCloudStorageManager:
    def __init__(self,  PROJECT_ID: str, BUCKET_NAME: str):
        self.storage_client = storage.Client(project=PROJECT_ID)
        self.BUCKET_NAME = BUCKET_NAME

    def bucket_exists(self) -> union(None, str):
        """To check if bucket exists

        Returns:
            None | str
        """
        return self.storage_client.lookup_bucket(self.BUCKET_NAME)

    def create_bucket(self) -> object:
        """To create new bucket in google cloud storage

        Returns:
            object: bucket object
        """
        return self.storage_client.create_bucket(self.BUCKET_NAME)

    def insert_bucket(self, dataframe: DataFrame, file_name: str) -> NoReturn:
        """To insert data into google cloud storage

        Args:
            dataframe (DataFrame): Dataframe to be inserted
            file_name (str): name of file to be stored

        Returns:
            NoReturn
        """
        bucket = self.storage_client.bucket(self.BUCKET_NAME)
        blob = bucket.blob(file_name)
        blob.upload_from_string(dataframe.to_csv(), 'csv')

    def get_lastest_data_from_bucket(self) -> str:
        """To get the last added data in bucket

        Returns:
            str: name of file
        """
        bucket = self.storage_client.bucket(self.BUCKET_NAME)
        blobs = bucket.list_blobs()
        most_recent_blob = max(blobs, key=lambda blob: blob.time_created)

        return most_recent_blob.name
