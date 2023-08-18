import pandas as pd
from pandas import DataFrame
from typing import List, NoReturn
from google.cloud import bigquery
from airflow.providers.google.cloud.sensors.bigquery import(
    BigQueryTableExistenceSensor
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator
)


class BigQueryDataManager():

    def __init__(self,  PROJECT_ID: str, DATASET_NAME: str, TABLE_NAME: str):
        self.client = bigquery.Client()
        self.PROJECT_ID = PROJECT_ID
        self.DATASET_NAME = DATASET_NAME
        self.TABLE_NAME = TABLE_NAME

    def get_table(self) -> NoReturn:
        """Function to check for table

        Returns:
            NoReturn
        """
        return BigQueryTableExistenceSensor(
            task_id="check_table_exists",
            project_id=self.PROJECT_ID,
            dataset_id=self.DATASET_NAME,
            table_id=self.TABLE_NAME
        )

    def create_table(self, schema: List[dict]) -> NoReturn:
        """To Create table

        Args:
            schema (List[dict]): Schema of table

        Returns:
            NoReturn
        """
        return BigQueryCreateEmptyTableOperator(
            task_id="create_table",
            dataset_id=self.DATASET_NAME,
            table_id=self.TABLE_NAME,
            schema_fields=schema
        )

    def insert_table(self, dataframe: DataFrame) -> str:
        """To insert data into table

        Args:
            dataframe (DataFrame): Dataframe to be inserted into table

        Returns:
            str: the new number of rows from table
        """
        table_id = f"{self.PROJECT_ID}.{self.DATASET_NAME}.{self.TABLE_NAME}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = self.client.load_table_from_dataframe(
            dataframe,
            table_id,
            job_config=job_config
        )
        job.result()
        table = self.client.get_table(table_id)
        result = f"New Ingestion into biqgery, total number of rows now - {table.num_rows} in {self.TABLE_NAME}"
        return result

    def run_query(self, query: str) -> List:
        """To run queries on Bigquery

        Args:
            query (str): query to be executed

        Returns:
            List: list of results from query
        """
        query_job = self.client.query(query)
        results = list(query_job.result())
        return results
