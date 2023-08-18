
import re
import json
import datetime
import requests
import pandas as pd
from typing import List
from pandas import DataFrame


def make_request(url: str) -> json:
    """To make api request to URL

    Args:
        url (str): link to be extracted

    Returns:
        json: json response from request
    """
    response = requests.get(url)
    return response.json()


def get_log_data_list(log: str) -> List:
    """To get log data in list by splitting through keyword

    Args:
        log (str): data to be split

    Returns:
        List: data split into lists
    """
    return log.lower().split("[hresult")


def get_message_list(log_data_list: str) -> List:
    """To get log data in list by splitting through "comma"

    Args:
        log_data_list (str): data to be split

    Returns:
        List:  data split into lists
    """
    return log_data_list[0].split(",")


def format_event_message(event_message_uncleaned: str) -> str:
    """To format event message by using regex

    Args:
        event_message_uncleaned (str): string to be formatted

    Returns:
        str: formatted message
    """
    pattern = 'failed.*'
    event_message = "".join(
            re.findall(pattern, event_message_uncleaned)
        )
    return event_message


def format_event_flag(message_string: str) -> str:
    """To format event flag by using regex

    Args:
        message_string (str): string to be formatted

    Returns:
        str: formatted message
    """
    pattern = '-.*'
    event_flag = "".join(
            re.findall(pattern, message_string)
        ).replace("-", "").replace("]", "").strip()
    return event_flag


def format_event_time_and_date(message_list: List) -> tuple:
    """To format date and time

    Args:
        message_list (List): string to be formatted

    Returns:
        tuple: tuple of formatted date and time
    """
    date_time_variable = message_list.split(" ")
    event_date = date_time_variable[0]
    event_time = date_time_variable[1]
    return event_date, event_time


def convert_str_to_date(date_string: str) -> datetime:
    """To convert sting to datetime object for analysis

    Args:
        date_string (str): datetime in str

    Returns:
        datetime: datetime object
    """
    date = date_string.split(",")[0]
    date_format = "%Y-%m-%d %H:%M:%S"
    return datetime.datetime.strptime(date, date_format)


def get_max_timestamp(log_df_filtered: DataFrame) -> datetime:
    """To get the highest timestamp from dataframe

    Args:
        log_df_filtered (DataFrame): Dataframe to be analysed

    Returns:
        max_timestamp(datetime):highest timestamp from dataframe
    """
    date_df = pd.DataFrame()
    date_df['dates'] = log_df_filtered['log'].apply(
            lambda x: convert_str_to_date(x)
        )
    date_df['log'] = log_df_filtered['log']
    max_timestamp = date_df['dates'].max()

    return max_timestamp
