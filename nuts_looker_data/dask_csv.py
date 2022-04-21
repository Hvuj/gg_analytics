import io
import os

import pandas as pd
import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import dask.dataframe as dd
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'F:\work\WM-Xplenty-Weekly\ex-nuts-f27c71f08060.json'
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
            raise Exception(f'There was an error in getting Access token!\n'
                            f'Please check credentials.')


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
client = bigquery.Client()


def delete_data_from_temp_table():
    # delete all rows from temp table
    query_delete_job = client.query(
        """
        DELETE
    FROM
      `ex-nuts.xplenty.looker_daily_data_staging`
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
          `ex-nuts.xplenty.looker_daily_data` AS
        SELECT
           *
        FROM
          `ex-nuts.xplenty.looker_daily_data_staging`;
        """
    )

    results_merge = query_job.result()  # Waits for job to complete.
    print('Data was overwritten\n', results_merge.path)


def run_task():
    project_id = "ex-nuts"
    dataset_name = 'xplenty'
    table_name = 'looker_daily_data'

    table_id = f"{project_id}.{dataset_name}.{table_name}_staging"

    # Create Table Schema
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True
    )

    column_names = [
        'ga_attribution_campaign',
        'ga_attribution_channel',
        'ga_attribution_medium',
        'orders_order_id',
        'orders_user_id',
        'orders_placed_date',
        'order_products_product_id',
        'order_skus_id',
        'contacts_contact_id',
        'users_is_user_account_created',
        'orders_net_revenue',
        'contacts_email_domain',
        'order_products_product_name',
        'order_products_merchandising_category',
        'order_products_first_tier_derpartment_name',
        'order_products_primary_department_name',
        'order_products_show_on_product_page_tag_names',
        'order_products_show_on_search_page_tag_names',
        'order_products_is_product_active',
        'order_skus_packaging_type',
        'ga_attribution_source',
        'orders_shipping_fee_amount',
        'orders_unique_merchandising_category_name',
        'orders_billed_to_city',
        'orders_billed_to_state_prov',
        'orders_billed_to_country_code'
        'order_skus_list_price',
        'order_skus_after_discount_price',
        'order_skus_before_discount_price',
        'contacts_first_subscribed_date',
        'contacts_first_subscribed_time',
        'orders_placed_time',
        'orders_is_placed_day_holiday',
        'order_skus_discount',
        'orders_canceled_date',
        'order_skus_weight',
        'kw_skus_data_net_weight',
        'order_skus_packaging_type_code',
        'gifting_is_gift',
        'orders_discount_fee_amount',
        'kw_skus_data_meltable',
        'orders_total_new_order_revenue',
        'order_skus_count_of_pieces',
        'order_skus_gross_profit',
        'accrued_gross_margin',
        'accrued_product_cost',
        'shipments_total_shipping_revenue',
        'order_skus_total_raw_material_cost',
        'order_skus_total_packaging_cost',
        'shipments_total_heat_resistant_packaging_cost',
        'order_skus_total_filling_labor_cost',
        'accrued_direct_labor_fulfillment_cost',
        'accrued_direct_labor_support_cost',
        'accrued_direct_labor_others_cost',
        'accrued_indirect_labor_cost',
        'accrued_freight_out_cost',
        'accrued_credit_card_processing_fee',
        'accrued_others_cost',
        'shipments_total_actual_carrier_cost',
        'orders_total_shipping_carrier_fees'
    ]

    check = True
    rows = 10001
    rows_to_skip = 0
    row_limit = 500

    dask_data = get_looker_data(access_token=get_looker_access_token(client_id=CLIENT_ID,
                                                                     client_secret=CLIENT_SECRET), look_id=2248,
                                limit=row_limit)

    # with open(path, 'w') as file:
    #     file.write(dask_data)
    #
    #     file.close()
    # data = dd.read_csv(dask_data)
    #
    # print(data)

    delete_data_from_temp_table()

    while check:

        rows_to_insert = dd.read_csv(io.StringIO(path.decode('utf-8')),
                                     sep=',',
                                     header=None,
                                     nrows=rows,
                                     skiprows=range(rows_to_skip),
                                     usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                                              21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38],
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

        rows_to_skip += 10000

        print(rows, '---rows')
        print(rows_to_skip, '---rows_to_skip')

        if rows_to_skip >= len(rows_to_insert):
            check = False

    merge_data()
    return print('done')


run_task()
