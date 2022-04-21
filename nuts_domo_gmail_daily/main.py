import base64
import io
import os

import pandas as pd
import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from get_access_token_method import get_access_token
from settings import EMAIL_FROM, EMAIL_TO

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'F:/work/Nuts/nuts_sa.json'

retries = Retry(total=15,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504])
requests.adapters.DEFAULT_RETRIES = retries
requests.adapters.DEFAULT_POOL_TIMEOUT = None
requests.adapters.HTTPAdapter(pool_connections=30, pool_maxsize=30, pool_block=False)

global message_id, attachment_id_status_code


def download_attachment_data(access_token: str, email_from: str, email_to: str):
    global attachment_id, message_id, attachment_id_status_code
    try:
        url = f'https://gmail.googleapis.com/gmail/v1/users/{email_from}/messages?q={email_to}'
        headers = {
            "Authorization": f'Bearer {access_token}',
            "Content-Type": "application/json"
        }
        payload = {}

        message_id = requests.get(url=url, headers=headers, data=payload).json()['messages'][0]['id']

        url = f'https://gmail.googleapis.com/gmail/v1/users/{email_from}/messages/{message_id}'

        response = requests.get(url=url, headers=headers, data=payload).json()['payload']['parts']

        for value in response:
            response = value['parts']

        for value in response:

            mime_type = value['mimeType']
            if mime_type == 'text/csv':
                attachment_id = value['body']['attachmentId']

        url = f'https://gmail.googleapis.com/gmail/v1/users/{email_from}/messages/{message_id}/attachments/{attachment_id}'
        response = requests.get(url=url, headers=headers, data=payload).json()['data']
        attachment_id_status_code = requests.get(url=url, headers=headers, data=payload).status_code

        return base64.b64decode(f'{response}')

    except Exception as error:
        print(f'There was an error in getting Access token!: {str(error)}')
        if error:
            raise Exception(f'There was an error in getting Access token!\n'
                            f'Please check credentials.')


def delete_mail(email_from: str, message_id: str, attachment_id_status_code: int, access_token: str):
    deletion_url = f'https://gmail.googleapis.com/gmail/v1/users/{email_from}/messages/{message_id}/trash'
    headers = {
        "Authorization": f'Bearer {access_token}',
        "Content-Type": "application/json"
    }
    payload = {}

    if attachment_id_status_code == 200:
        try:
            status_code = requests.post(url=deletion_url, headers=headers, data=payload).status_code
            if status_code == 200:
                print('Email was deleted')
                return status_code

        except Exception as deletion_error:
            print(f'There was an error deleting the email!: {str(deletion_error)}')
            if deletion_error:
                raise Exception(f'Please check the error')


"""
 Get bq data in order to get the latest date
"""
client = bigquery.Client()


def delete_data_from_temp_table():
    try:
        # delete all rows from temp table
        query_delete_job = client.query(
            """
            DELETE
        FROM
          `ex-nuts.xplenty.domo_data_1_staging`
        WHERE
          TRUE;
            """
        )

        # get the results
        results = query_delete_job.result()  # Waits for job to complete.
        print('Data was deleted from temp table.')
    except Exception as error:
        print(f'There was an error in getting deleting data from temp table in big query: {str(error)}')
        if error:
            raise Exception(f'There was an error in getting deleting data from temp table in big query')


def merge_data():
    try:
        # query the table
        query_job = client.query(
            """
            MERGE
              `ex-nuts.xplenty.domo_data_1` T
            USING
              `ex-nuts.xplenty.domo_data_1_staging` S
            ON
              (T.Day=S.Day )
              WHEN MATCHED THEN UPDATE SET T.Day=S.Day, T.Spend_Type=S. Spend_Type, T.Spend=S.Spend
              WHEN NOT MATCHED
              THEN
            INSERT
              ( Day,
                Spend_Type,
                Spend )
            VALUES
              ( S.Day, S.Spend_Type, S.Spend )
            """
        )

        results_merge = query_job.result()  # Waits for job to complete.
        print('Data was overwritten\n', results_merge.path)

    except Exception as error:
        print(f'There was an error in merging data from temp table to final table in big query: {str(error)}')
        if error:
            raise Exception(f'This is probably because of service account credentials or table/s not existing at all')


def run_task():
    data = download_attachment_data(get_access_token(), EMAIL_FROM, EMAIL_TO)
    project_id = "ex-nuts"
    dataset_name = 'xplenty'
    table_name = 'domo_data_1'

    table_id = f"{project_id}.{dataset_name}.{table_name}_staging"

    # Create Table Schema
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True
    )
    column_names = [
        'Day',
        'Spend_Type',
        'Spend'
    ]

    delete_data_from_temp_table()

    try:
        rows_to_insert = pd.read_csv(io.StringIO(data.decode('utf-8')),
                                     sep=',',
                                     header=None,
                                     usecols=[0, 1, 2],
                                     names=column_names)

        print('started to push data into bq')

        dataframe = pd.DataFrame(rows_to_insert)

        job = client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

        merge_data()

        delete_mail(email_from=EMAIL_FROM, attachment_id_status_code=attachment_id_status_code, message_id=message_id,
                    access_token=get_access_token())

    except Exception as error:
        print(f'Error! was not able to push data into big query: {str(error)}')
        if error:
            raise Exception(f'Please check the source')


run_task()
