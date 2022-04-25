import io
import os

import pandas as pd
import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import dask.dataframe as dd

retries = Retry(total=15,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504])
requests.adapters.DEFAULT_RETRIES = retries
requests.adapters.DEFAULT_POOL_TIMEOUT = None
requests.adapters.HTTPAdapter(pool_connections=30, pool_maxsize=30, pool_block=False)
path = 'data.csv'

from settings import CLIENT_ID, CLIENT_SECRET


def get_looker_access_token(client_id: str, client_secret: str) -> str:
    uri = "https://nuts.looker.com/api/3.1/login"
    payload = f'client_id={client_id}&client_secret={client_secret}'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    try:
        response = requests.post(url=uri,
                                 headers=headers,
                                 data=payload,
                                 verify=True,
                                 timeout=600,
                                 stream=True).json()['access_token']

        print('Access token granted!')
        return response

    except Exception as error:
        print(f'There was an error in getting Access token!: {str(error)}')
        if error:
            raise Exception('There was an error in getting Access token!\\n') from error


def get_looker_data(access_token: str, look_id: int, limit: int = 500):
    uri = f'https://nuts.looker.com/api/3.1/looks/{look_id}/run/csv?limit={limit}'
    payload = {}
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    try:
        response = requests.get(url=uri,
                                headers=headers,
                                data=payload,
                                verify=True,
                                timeout=600 * 60,
                                stream=True)
        print('Success! Data was received.')

        return response.text

    except Exception as error:
        print(f'There was an error in getting Data! {str(error)}')


"""
 Get bq data in order to get the latest date
"""
client = bigquery.Client("ex-nuts")


def delete_data_from_temp_table():
    # delete all rows from temp table
    query_delete_job = client.query(
        """
        DELETE
    FROM
      `ex-nuts.xplenty.looker_daily_shipment_data_staging`
    WHERE
      TRUE;
        """
    )

    # get the results
    results = query_delete_job.result()  # Waits for job to complete.
    print('Data was deleted from temp table. ', results)


def merge_data():
    # query the table
    query_job = client.query(
        """
        CREATE OR REPLACE TABLE
          `ex-nuts.xplenty.looker_daily_shipment_data` AS
        SELECT
           *
        FROM
          `ex-nuts.xplenty.looker_daily_shipment_data_staging`;
        """
    )

    results_merge = query_job.result()  # Waits for job to complete.
    print('Data was overwritten\n', results_merge.path)


def run_task():
    project_id = "ex-nuts"
    dataset_name = 'xplenty'
    table_name = 'looker_daily_shipment_data'

    table_id = f"{project_id}.{dataset_name}.{table_name}_staging"

    # Create Table Schema
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True
    )

    column_names = [
        'order_id',
        'shipment_id',
        'shipped_time',
        'shipped_date',
        'orders_discount_fee_amount',
        'orders_is_reshipment',
        'shipments_is_shipment_canceled',
        'shipments_canceled_date',
        'credit_card_processing_fee',
        'direct_labor_fulfillment_cost',
        'direct_labor_support_cost',
        'direct_labor_others_cost',
        'indirect_labor_cost',
        'others_cost',
        'total_heat_res_packaging_cost',
        'total_shipment_revenue',

    ]

    data_types = {
        'order_id': 'string',
        'shipment_id': 'string',
        'shipped_time': 'string',
        'shipped_date': 'string',
        'orders_discount_fee_amount': 'float',
        'orders_is_reshipment': 'string',
        'shipments_is_shipment_canceled': 'string',
        'shipments_canceled_date': 'string',
        'credit_card_processing_fee': 'float',
        'direct_labor_fulfillment_cost': 'float',
        'direct_labor_support_cost': 'float',
        'direct_labor_others_cost': 'float',
        'indirect_labor_cost': 'float',
        'others_cost': 'float',
        'total_heat_res_packaging_cost': 'float',
        'total_shipment_revenue': 'float',
    }

    check = True
    rows = 3000001
    rows_to_skip = 0
    row_limit = 10000000

    dask_data = get_looker_data(access_token=get_looker_access_token(client_id=CLIENT_ID,
                                                                     client_secret=CLIENT_SECRET), look_id=2332,
                                limit=row_limit)

    with open(path, 'w') as file:
        file.write(dask_data)

        file.close()

    # delete_data_from_temp_table()

    rows_to_insert = dd.read_csv(path, sep=',', names=column_names, header=0, dtype=data_types).head(n=rows)

    print('started to push data into bq')

    job = client.load_table_from_dataframe(
        rows_to_insert, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    rows_to_skip += 1000000

    print(rows, '---rows')
    print(rows_to_skip, '---rows_to_skip')

    # merge_data()
    return print('done')


run_task()
