import pandas as pd
from datetime import datetime
from datetime import timedelta
from pandas import DataFrame
from typing import NoReturn
from airflow import AirflowException
from airflow.decorators import dag, task
from helpers.bigquery_helper import BigQueryDataManager
from helpers.gcs_helper import GoogleCloudStorageManager
from airflow.utils.email import send_email
from utils import (
    format_event_flag, format_event_message, format_event_time_and_date,
    get_message_list, get_log_data_list, make_request, get_max_timestamp
)
from google.cloud.exceptions import NotFound
from helpers.constants import PROJECT_ID, DATASET_NAME, TABLE_NAME, BUCKET_NAME


default_args = {
    'owner': 'airflow',
    'depends_on_past': 'False',
    'start_date': datetime(2023, 8, 18),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'schedule_interval': '@daily',
    'retry_delay': timedelta(seconds=20)
}


@dag(default_args=default_args, catchup=False)
def server_report_dag():

    @task
    def extract_and_save_raw_data() -> DataFrame:
        """To extract data from URL and save in bucket

        Returns:
            Dataframe: Extracted Data in dataframe
        """
        url = "https://github.com/logpai/loghub/blob/master/Windows/Windows_2k.log"

        log_data_json = make_request(url)
        log_data = log_data_json['payload']['blob']['rawLines']
        log_df = pd.DataFrame(log_data, columns=['log'])
        log_df_filtered = log_df[log_df['log'].str.contains('Failed')]
        max_timestamp = get_max_timestamp(log_df_filtered)
        file_name = f"{max_timestamp.strftime('%Y-%m-%d %H:%M:%S')}.csv"

        storage_cli = GoogleCloudStorageManager(PROJECT_ID, BUCKET_NAME)
        try:
            storage_cli.bucket_exists()
        except NotFound:
            storage_cli.create_bucket()
        storage_cli.insert_bucket(log_df_filtered, file_name)

        return log_df_filtered

    @task
    def transform_data(log_df: DataFrame) -> DataFrame:
        """To transform Dataframe

        Args:
            log_df (DataFrame): DataFrame to be transformed

        Returns:
            DataFrame: Processed Dataframe
        """
        transformed_df = pd.DataFrame()

        transformed_df['Event_message'] = log_df['log'].apply(
            lambda x: format_event_message(
                get_message_list(get_log_data_list(x))[1]
            )
        )
        transformed_df['Event_Flag'] = log_df['log'].apply(
            lambda x: format_event_flag(get_log_data_list(x)[1])
        )
        transformed_df['Event_Status'] = "Failed"
        transformed_df[['Event_date', 'Event_time']] = log_df['log'].apply(
            lambda x: format_event_time_and_date(
                get_message_list(get_log_data_list(x))[0]
            )
        ).apply(pd.Series)

        return transformed_df


    @task
    def bigquery_load_data(formatted_data: DataFrame) -> NoReturn:
        """To load data into bigquery and send emails

        Args:
            formatted_data (DataFrame): Cleaned Data

        Returns:
            NoReturn
        """

        try:
            bigquery_client = BigQueryDataManager(
                PROJECT_ID, DATASET_NAME, TABLE_NAME
            )
            if bigquery_client.get_table():
                pass
            else:
                schema = [
                    {"name": "Event_time", "type": "DATE", "mode": "REQUIRED"},
                    {"name": "Event_date", "type": "TIME", "mode": "REQUIRED"},
                    {"name": "Event_status", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "Event_message", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "Event_flag", "type": "STRING", "mode": "REQUIRED"}
                ]
                bigquery_client.create_table(schema)

            msg = bigquery_client.insert_table(formatted_data)

        except AirflowException as error:
            msg = error

        send_email(
                to="priyegeorge1st@gmail.com",
                subject="UPDATE ON DATA RUN",
                html_content=f"""<table>
                {msg}
            </table>
            """,
            )

    # Define the DAG structure
    bigquery_load_data(transform_data(extract_and_save_raw_data()))


server_report_dag()
