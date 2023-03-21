from __future__ import annotations
from typing import Final, Any, List, Dict, Optional, TypedDict, Tuple, Union
from datetime import timedelta, date, datetime
from get_google_access_token_module import get_google_access_token
from settings import ciphered_refresh_token_text, ciphered_redirect_uri_text, ciphered_client_id_text, \
    ciphered_client_secret_text, cipher_suite
from upload_to_bq import upload_data_to_bq
from requests.adapters import HTTPAdapter, Retry
from google_analytics_schemas import _SESSION_DIMENSIONS, _TRANSACTION_DIMENSIONS, _SESSION_METRICS, \
    _TRANSACTION_METRICS
from bq_functions import combine_data, drop_table
import requests
import pandas as pd
import dask.dataframe as dd
import concurrent.futures as cf
import multiprocessing as mp
import asyncio
import time
import aiohttp
import json
import logging

try:
    deciphered_refresh_token = cipher_suite.decrypt(ciphered_refresh_token_text).decode()
    deciphered_redirect_uri = cipher_suite.decrypt(ciphered_redirect_uri_text).decode()
    deciphered_client_id = cipher_suite.decrypt(ciphered_client_id_text).decode()
    deciphered_client_secret = cipher_suite.decrypt(ciphered_client_secret_text).decode()
except Exception as e:
    logging.error("An error occurred while decrypting the data: %s", e)
    raise

# Check if decryption is successful
if None in (deciphered_refresh_token, deciphered_redirect_uri, deciphered_client_id, deciphered_client_secret):
    raise ValueError(
        "Missing environment variables."
        "Make sure to set:"
        "deciphered_refresh_token, deciphered_redirect_uri, deciphered_client_id, deciphered_client_secret.")

print("Encryption and decryption successful!")
# try:
#     # Decrypt the key using KMS
#     key_decryption_response = kms_client.decrypt(
#         request={
#             "name": KEY_NAME,
#             "ciphertext": encrypted_key
#         }
#     )
#     # Get the plaintext
#     decrypted_key = key_decryption_response.plaintext
#
#     # Decrypt the data
#     decryption_cipher = Fernet(decrypted_key)
#     deciphered_refresh_token = decryption_cipher.decrypt(ciphered_refresh_token_text).decode()
#     deciphered_redirect_uri = decryption_cipher.decrypt(ciphered_redirect_uri_text).decode()
#     deciphered_client_id = decryption_cipher.decrypt(ciphered_client_id_text).decode()
#     deciphered_client_secret = decryption_cipher.decrypt(ciphered_client_secret_text).decode()
# except Exception as e:
#     logging.error("An error occurred while decrypting the data: %s", e)
#     raise
#
# # Check if decryption is successful
# if None in (deciphered_refresh_token, deciphered_redirect_uri, deciphered_client_id, deciphered_client_secret):
#     raise ValueError(
#         "Missing environment variables."
#         "Make sure to set:"
#         "deciphered_refresh_token, GOOGLE_REDIRECT_URI, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET.")
#
# print("Encryption and decryption successful!")

session = requests.Session()

retry = Retry(total=15,
              connect=10,
              read=10,
              status=10,
              backoff_factor=1,
              status_forcelist=[400, 500, 502, 503, 504],
              allowed_methods=["GET", "POST"],
              raise_on_status=False)

adapter = HTTPAdapter(pool_connections=15,
                      pool_maxsize=15,
                      pool_block=False,
                      max_retries=retry)
session.mount("https://", adapter)


async def batch_get_google_analytics_data(list_of_data, view_ids_list) -> Tuple:
    """
    A function that makes concurrent API calls to the Google Analytics API.
    :param list_of_data: A list of dictionaries representing the payload for each API call.
    :return: A list of responses from the API calls.
    """
    # Get the access token required to make the API calls
    access_token: Final[str] = get_google_access_token(client_id=deciphered_client_id,
                                                       client_secret=deciphered_client_secret,
                                                       redirect_uri=deciphered_redirect_uri,
                                                       refresh_token=deciphered_refresh_token)

    # The url for the Google Analytics API
    url: Final[str] = 'https://analyticsreporting.googleapis.com/v4/reports:batchGet'
    # The headers required for the API call
    headers: Final[TypedDict[str, str]] = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    async with aiohttp.ClientSession(headers=headers) as async_session:
        # Create a list of tasks to be executed concurrently
        tasks = []
        for payload in list_of_data:
            # if payload['reportRequests'][0]['viewId'] == view_ids_list[0][_iter]:
            #     print(view_ids_list[0][_iter])
            #     print(view_ids_list[1][_iter])
            #     account_name=view_ids_list[1][_iter]
            # for item in view_ids_list:
            #     if payload['reportRequests'][0]['viewId'] == item[0]
            #         account_name = item[1]

            account_name = "_Pivotal Home Solutions" \
                if payload['reportRequests'][0]['viewId'] == [item[0] for item in view_ids_list][0] else "AWR"
            task = asyncio.ensure_future(process_batch_get_google_analytics_data(async_session=async_session,
                                                                                 url=url,
                                                                                 payload=payload))
            tasks.append(task)
        # Gather the results of all the tasks and return them
        return await asyncio.gather(*tasks, return_exceptions=True), account_name


async def process_batch_get_google_analytics_data(async_session, url: str, payload: Dict[str, Any]):
    """
    Make a post request to the provided url with the given payload.
    The function uses a while loop to check for the presence of a 'nextPageToken'
    in the response data, and if present, updates the payload and makes another
    request to retrieve the next page of data.
    :param async_session: aiohttp.ClientSession
    :param url: str
    :param payload: Dict[str, Any]
    :return: Coroutine
    """
    reports: List = []
    while True:
        try:
            async with async_session.post(url=url, data=json.dumps(payload)) as async_response:
                if not 200 <= async_response.status < 300:
                    raise ValueError(f"Failed to fetch data: {await async_response.json()}")
                data = await async_response.json()

                # if next_page_token := data['reports'][0].get('nextPageToken'):
                #     payload['reportRequests'][0]['pageToken'] = next_page_token
                # else:
                #     return data

                for report in data['reports']:
                    reports.append(report)
                    if next_page_token := report.get('nextPageToken'):
                        payload['reportRequests'][0]['pageToken'] = next_page_token
                        continue
                break
        except Exception as e:
            print(f"Error occurred: {e}")
            time.sleep(3)
    return reports


def date_range(start_date: str, end_date: str) -> List[str]:
    """
    This function takes in two arguments, start_date and end_date, both of which should be strings of format "YYYY-MM-DD".
    It then creates a list of dates between the start_date and end_date and returns the list.

    :param start_date: The starting date of the range.
    :type start_date: str
    :param end_date: The ending date of the range.
    :type end_date: str
    :return: A list of dates between the start_date and end_date in the format of "YYYY-MM-DD"
    :rtype: List[str]
    :raises ValueError: If the date format is incorrect
    """

    try:
        date_list: List[str] = []
        start_date: datetime = datetime.strptime(start_date, "%Y-%m-%d")
        end_date: datetime = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as val_error:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD") from val_error

    current_date: datetime = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    return date_list


def convert_date(date_string: str) -> str:
    """
    This function takes a date in the format "YYYYMMDD" and converts it to "YYYY-MM-DD"
    :param date_string: date in format "YYYYMMDD"
    :type date_string: str
    :return: date in format "YYYY-MM-DD"
    :rtype: str
    """
    date_obj: Final[datetime] = datetime.strptime(date_string, "%Y-%m-%d")
    return date_obj.strftime("%Y%m%d")


def process_data(batched_data: List[Dict], data_type: str, account_name: str) -> dd.DataFrame:
    """
    This function takes in two arguments, batched_data and data_type, which should be a list of dictionaries and a string respectively.
    The function extracts certain key-value pairs from the nested dictionaries and
    returns a distributed dataframe using the dask library.

    :param batched_data: a list of dictionaries containing the data to be processed
    :type batched_data: List[Dict]
    :param data_type: a string to specify if the data is session or transaction
    :type data_type: str
    :return: a distributed dataframe containing the extracted data
    :rtype: dd.DataFrame
    """
    wanted_data_list: List[Optional[Dict[str, Any]]] = []

    for batch_data in batched_data:
        for item in batch_data:
            for dim in item.get('data', {}).get('rows', []):
                try:
                    if data_type == 'session':
                        data_to_extract: Dict[str, str] = {
                            "client_id": dim['dimensions'][0],
                            "dateHourMinute": dim['dimensions'][1],
                            "ga_source_medium": dim['dimensions'][2],
                            "ga_adwordscampaign": dim['dimensions'][3],
                            # "ga_campaign": dim['dimensions'][4],
                            "sessions": dim['metrics'][0]['values'][0],

                            # "date": dim['dimensions'][0],
                            # "account_name": f"{account_name}",
                            # "ga_sourceMedium": dim['dimensions'][1],
                            # "ga_adwordsCampaignID": dim['dimensions'][2],
                            # "ga_campaign": dim['dimensions'][3],
                            # "ga_goal14Completions": dim['metrics'][0]['values'][0],
                            # "ga_goal3Completions": dim['metrics'][0]['values'][1],
                            # "ga_newUsers": dim['metrics'][0]['values'][2],
                            # "ga_sessions": dim['metrics'][0]['values'][3],
                        }
                    elif data_type == 'transaction':
                        data_to_extract: Dict[str, str] = {
                            "client_id": dim['dimensions'][0],
                            "dateHourMinute": dim['dimensions'][1],
                            "ga_source_medium": dim['dimensions'][2],
                            "ga_adwordscampaign": dim['dimensions'][3],
                            # "ga_campaign": dim['dimensions'][4],
                            "ga_transactionId": dim['dimensions'][4],
                            "transactions": dim['metrics'][0]['values'][0],

                            # "date": dim['dimensions'][0],
                            # "account_name": f"{account_name}",
                            # "ga_sourceMedium": dim['dimensions'][1],
                            # "ga_campaign": dim['dimensions'][2],
                            # "ga_transactionId": dim['dimensions'][3],
                            # "ga_transactions": dim['metrics'][0]['values'][0],
                            # "ga_transactionRevenue": dim['metrics'][0]['values'][1],
                        }
                    else:
                        raise ValueError(
                            f"Invalid data_type: {data_type}. data_type should be either session or transaction")
                    wanted_data_list.append(data_to_extract)
                except (KeyError, ValueError, TypeError) as error:
                    print(f"There was an error while processing or it`s ok "
                          f"and there is no data at the moment for the current batch:{error}")
    return dd.from_pandas(pd.DataFrame(wanted_data_list), npartitions=len(wanted_data_list))


def split_data_into_chunks(data_to_split: List, num_of_splits: int) -> Union[List, str]:
    list_of_chunks: Final[List] = []
    try:

        list_of_chunks.extend(data_to_split[start_num: start_num + num_of_splits] for start_num in
                              range(0, len(data_to_split), num_of_splits))

        return list_of_chunks or 'There is no data to send - please check sql logic'
    except Exception as split_data_into_chunks_error:
        raise print(split_data_into_chunks_error) from split_data_into_chunks_error


def prepare_data(start_date: str,
                 end_date: str,
                 data_type: str,
                 minute_interval: str,
                 view_id: str) -> List[Dict[str, List[Dict[str, Any]]]]:
    """
    This function, prepare_data, takes in three arguments: start_date, end_date and view_id.
    It creates a list of requests, which are then used to call the Google Analytics API.
    The requests are based on a date range specified by the input parameters, and are filtered by hour and minute.

    :param start_date: The starting date of the range in the format of "YYYY-MM-DD"
    :type start_date: str
    :param end_date: The ending date of the range in the format of "YYYY-MM-DD"
    :type end_date: str
    :param data_type: a string to specify if the data is session or transaction
    :type data_type: str
    :param view_id: The unique identifier for the view in Google Analytics.
    :type view_id: str
    :return: list of requests
    :rtype: List[Dict[str, List[Dict[str, Any]]]]
    :raises ValueError: If the date format is incorrect
    """
    global a
    try:
        request_list: Final[List[TypedDict[str, List[TypedDict[str, Any]]]]] = []
        if minute_interval == '5_minutes':
            regex_minute_list: Final[List[str, str, str]] = ['0-5', '6-9']
        else:
            regex_minute_list: Final[List[str, str, str]] = ['0-1', '2-3', '4-5']

        for new_date in date_range(start_date=start_date, end_date=end_date):
            for hour in range(24):
                if minute_interval == '5_minutes':
                    for minute in regex_minute_list:
                        batched_req: List[Dict[str, Any]] = []
                        for _ in range(10):
                            if hour < 10:
                                a = f"{convert_date(new_date)}0{hour}{_}[{minute}]"
                            else:
                                a = f"{convert_date(new_date)}{hour}{_}[{minute}]"

                            payload_to_insert: Dict[str, Any] = {
                                "viewId": f"{view_id}",
                                "orderBys": [
                                    {
                                        "fieldName": "ga:dateHourMinute",
                                        "orderType": "VALUE",
                                        "sortOrder": "ASCENDING"
                                    }
                                ],
                                "dateRanges": [
                                    {
                                        "startDate": f"{new_date}",
                                        "endDate": f"{new_date}"
                                    }
                                ],
                                "filtersExpression": f"ga:dateHourMinute=~{a}",
                                "pageSize": 100000,
                                "samplingLevel": "LARGE",
                            }
                            if data_type == 'session':
                                payload_to_insert['dimensions'] = _SESSION_DIMENSIONS
                                payload_to_insert['metrics'] = _SESSION_METRICS
                            else:
                                payload_to_insert['dimensions'] = _TRANSACTION_DIMENSIONS
                                payload_to_insert['metrics'] = _TRANSACTION_METRICS

                            batched_req.append(payload_to_insert)

                        split_arr = split_data_into_chunks(batched_req, 5)
                        for arr in split_arr:
                            payload: Dict[str, List[Dict[str, Any]]] = {
                                "reportRequests": arr
                            }

                            request_list.append(payload)

                else:
                    for minute in regex_minute_list:
                        if hour < 10:
                            a = f"{convert_date(new_date)}0{hour}[{minute}][0-9]"
                        else:
                            a = f"{convert_date(new_date)}{hour}[{minute}][0-9]"

                        payload_to_insert: Dict[str, Any] = {
                            "viewId": f"{view_id}",
                            "orderBys": [
                                {
                                    "fieldName": "ga:dateHourMinute",
                                    "orderType": "VALUE",
                                    "sortOrder": "ASCENDING"
                                }
                            ],
                            "dateRanges": [
                                {
                                    "startDate": f"{new_date}",
                                    "endDate": f"{new_date}"
                                }
                            ],
                            "filtersExpression": f"ga:dateHourMinute=~{a}",
                            "pageSize": 100000,
                            "samplingLevel": "LARGE",
                        }
                        if data_type == 'session':
                            payload_to_insert['dimensions'] = _SESSION_DIMENSIONS
                            payload_to_insert['metrics'] = _SESSION_METRICS
                        else:
                            payload_to_insert['dimensions'] = _TRANSACTION_DIMENSIONS
                            payload_to_insert['metrics'] = _TRANSACTION_METRICS

                        batched_req.append(payload_to_insert)
                    payload: Dict[str, List[Dict[str, Any]]] = {
                        "reportRequests": batched_req
                    }

                    request_list.append(payload)
        return request_list
    except ValueError as val_error:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD") from val_error


def prepare_data_by_day(start_date: str,
                        end_date: str,
                        data_type: str,
                        view_id: str) -> List[Dict[str, List[Dict[str, Any]]]]:
    """
    This function, prepare_data_by_day, takes in three arguments: start_date, end_date and view_id.
    It creates a list of requests, which are then used to call the Google Analytics API.
    The requests are based on a date range specified by the input parameters, and are filtered by day.

    :param start_date: The starting date of the date range in the format "YYYY-MM-DD"
    :type start_date: str
    :param end_date: The ending date of the date range in the format "YYYY-MM-DD"
    :type end_date: str
    :param data_type: a string to specify if the data is session or transaction
    :type data_type: str
    :param view_id: The unique identifier for the view in Google Analytics.
    :type view_id: str
    :return: list of requests
    :rtype: List[Dict[str, List[Dict[str, Any]]]]
    """

    try:
        request_list: Final[List[TypedDict[str, List[TypedDict[str, Any]]]]] = []
        for new_date in date_range(start_date=start_date, end_date=end_date):
            payload_to_insert: Dict[str, Any] = {
                "viewId": f"{view_id}",
                "orderBys": [
                    {
                        "fieldName": "ga:dateHourMinute",
                        # "fieldName": "ga:date",
                        "orderType": "VALUE",
                        "sortOrder": "ASCENDING",
                    }
                ],
                "dateRanges": [
                    {
                        "startDate": f"{new_date}",
                        "endDate": f"{new_date}",
                    }
                ],
                "pageSize": 100000,
                "samplingLevel": "LARGE",
            }
            if data_type == 'session':
                payload_to_insert['dimensions'] = _SESSION_DIMENSIONS
                payload_to_insert['metrics'] = _SESSION_METRICS
            else:
                payload_to_insert['dimensions'] = _TRANSACTION_DIMENSIONS
                payload_to_insert['metrics'] = _TRANSACTION_METRICS
            batched_req: List[Dict[str, Any]] = [payload_to_insert]
            payload: Dict[str, Any] = {
                "reportRequests": batched_req
            }
            request_list.append(payload)
        return request_list
    except ValueError as val_error:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD") from val_error


async def main(start_date: Optional[str],
               end_date: Optional[str],
               data_type: str,
               view_ids: List[Tuple[str, str]],
               project_id: str,
               dataset_name: str,
               table_name: str,
               interval_minutes: str,
               **kwargs) -> Optional[bool]:
    """
    This function, main, is an asynchronous function which takes in keyword arguments as input.
    It uses the time_increment keyword argument to determine whether to call the prepare_data_by_day or
    prepare_data function.
    It then chunks the returned list of dates and makes an API call for each chunk of dates,
    using the batch_get_google_analytics_data function.
    The results of the API call are then passed to the push_to_bq_in_parallel function.

    :param start_date: The start date of the range in the format "YYYY-MM-DD"
    :type start_date: str
    :param end_date: The end date of the range in the format "YYYY-MM-DD"
    :type end_date: str
    :param data_type: a string to specify if the data is session or transaction
    :type data_type: str
    :param kwargs: keyword arguments, including time_increment and view_id
    :type kwargs: Any
    :return: return True if the function completes successfully, otherwise raises an error
    :rtype: Optional[bool]
    """
    list_of_requests: List[TypedDict[str, str]] = []
    try:
        if kwargs['time_increment'] == 'day':
            list_of_requests.extend(
                prepare_data_by_day(start_date, end_date, data_type, view_id[0])
                for view_id in view_ids
            )
        else:
            list_of_requests.extend(
                prepare_data(start_date, end_date, data_type, interval_minutes, view_id[0])
                for view_id in view_ids
            )
        chunk_size: Final[int] = 3 if kwargs['time_increment'] == 'hour' else 10
        for item in list_of_requests:
            for api_chunk in range(0, len(item), chunk_size):
                chunk: List[str, Any] = item[api_chunk:api_chunk + chunk_size]
                api_results: Tuple = await batch_get_google_analytics_data(chunk, view_ids)
                account_name = api_results[1]

                push_to_bq_in_parallel([api_results[0]],
                                       data_type,
                                       account_name,
                                       project_id,
                                       dataset_name,
                                       table_name)
        return True
    except Exception as error:
        raise error


def push_to_bq_in_parallel(batches_data: List[Any],
                           data_type: str,
                           account_name: str,
                           project_id: str,
                           dataset_name: str,
                           table_name: str) -> None:
    """
    This function, push_to_bq_in_parallel, takes in a single argument, batches_data, which is a list of data.
    The function uses the concurrent.
    futures library to process the data in parallel using a thread pool executor,
    with the number of workers equal to the number of CPU cores plus four.
    The processed data is then concatenated and computed using the dask library,
    and the resulting dataframe is then passed to the upload_data_to_bq function to upload the data to Google BigQuery.

    :param batches_data: a list of data to be processed
    :type batches_data: List
    :param data_type: a string to specify if the data is session or transaction
    :type data_type: str
    :return: None
    """
    max_cpu_count: Final[int] = int(mp.cpu_count())

    with cf.ThreadPoolExecutor(max_workers=max_cpu_count + 4) as mp_executor:
        data_results = [mp_executor.submit(process_data, sec, data_type, account_name) for sec in batches_data]
        thread_data = [f.result() for f in cf.as_completed(data_results)]

        data_to_send = dd.concat(thread_data).compute(scheduler="processes")
        # data_to_send['date'] = dd.to_datetime(data_to_send['date']).dt.strftime("%Y-%m-%d")

        try:
            upload_data_to_bq(project_id=project_id,
                              dataset_name=dataset_name,
                              table_name=table_name,
                              data_to_send=data_to_send,
                              data_type=data_type)
            # if data_type == 'transaction':
            #     time.sleep(5)
            # time.sleep(5)
        except ValueError as missing_data:
            print(missing_data)


def run(request: Optional[Any]) -> str:
    """
    This function run takes in a single argument request which is an optional parameter.
    It runs the coroutine main and the function returns a string 'True' if the returned value is a boolean 'True'
    otherwise it returns 'False'.

    :param request: an optional parameter that is not being used in the function
    :type request: Optional[Any]
    :return: a string 'True' if the returned value from the coroutine is boolean 'True' otherwise it returns 'False'
    :rtype: str
    """
    # start_date = request.get_json().get('start_date')
    # end_date = request.get_json().get('end_date')
    # data_type = request.get_json().get('data_type')
    # time_increment = request.get_json().get('time_increment')
    # view_ids_string = request.get_json().get('view_ids')
    # view_ids = [ast.literal_eval(item) for item in view_ids_string]
    # project_id = request.get_json().get('project_id')
    # dataset_name = request.get_json().get('dataset_name')

    data_type = 'session'
    date_field: Final[str] = 'date'
    time_increment = 'hour'
    project_id: Final[str] = 'ex-colehaan'
    dataset_name: Final[str] = 'xplenty'
    # table_name: Final[str] = f'ga_{data_type}_dataQ4'
    table_name: Final[str] = f'ga_markov_chain_data_{data_type}2'
    # start_date = '2022-10-01'
    # end_date = '2022-12-31'
    interval_minutes: Final[str] = '5_minutes'
    # interval_minutes:Final[str]='20_minutes'

    start_date = '2022-09-28'
    end_date = '2022-10-01'

    # delete temp table
    drop_table(project_id=project_id,
               dataset_name=dataset_name,
               table_name=table_name)
    if None in (start_date, end_date):
        print("start_date and end_date are None - using default 30 day retention policy")
        date_type = "%Y-%m-%d"
        yesterday = (datetime.now() - timedelta(days=1)).strftime(date_type)
        last_30_days = (datetime.now() - timedelta(days=36)).strftime(date_type)

        # Run the coroutine
        batches_data = asyncio.run(main(start_date=last_30_days,
                                        end_date=yesterday,
                                        data_type=data_type,
                                        time_increment=time_increment,
                                        view_ids=[
                                            # ('74867711', '_Pivotal Home Solutions'),
                                            # ('98664327', 'AWR')
                                            # ('5731', 'Nuts')
                                        ],
                                        project_id=project_id,
                                        dataset_name=dataset_name,
                                        table_name=table_name,
                                        interval_minutes=interval_minutes))
    else:

        # Run the coroutine
        batches_data = asyncio.run(main(start_date=start_date,
                                        end_date=end_date,
                                        data_type=data_type,
                                        time_increment=time_increment,
                                        view_ids=[
                                            # ('74867711', '_Pivotal Home Solutions'),
                                            # ('98664327', 'AWR')
                                            # ('5731', 'Nuts'),
                                            # ('209345657', 'omaze')
                                            ('84824134', 'colehaan')
                                        ],
                                        project_id=project_id,
                                        dataset_name=dataset_name,
                                        table_name=table_name,
                                        interval_minutes=interval_minutes))

    # combine_data(project_id=project_id,
    #              dataset_name=dataset_name,
    #              table_name=table_name,
    #              date_field=date_field)

    return 'True' if isinstance(batches_data, bool) else 'False'


run('yes')
