from datetime import datetime, timedelta
import os
import json
import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from requests_oauthlib import OAuth2Session
from urllib3.util.retry import Retry
import dask.dataframe as dd
from settings import CLIENT_ID, CLIENT_SECRET
from urllib3.exceptions import HTTPError
from oauthlib.oauth2 import BackendApplicationClient

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'F:\work\WM-Xplenty-Weekly\ex-nuts-f27c71f08060.json'
retries = Retry(total=15,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504])
requests.adapters.DEFAULT_RETRIES = retries
requests.adapters.DEFAULT_POOL_TIMEOUT = None
requests.adapters.HTTPAdapter(pool_connections=15, pool_maxsize=15, pool_block=False)

session = requests.Session()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
path = 'data.csv'
file_path = f'/tmp/{path}'
path = os.path.join(BASE_DIR, file_path)
print(path)

def get_criteo_access_token(client_id: str, client_secret: str) -> str:
    try:
        client = BackendApplicationClient(client_id=client_id)
        oauth = OAuth2Session(client=client)
        token = oauth.fetch_token(token_url='https://api.criteo.com/oauth2/token', client_id=client_id,
                                  client_secret=client_secret)

        return token['access_token']

    except Exception as error:
        print(f'There was an error in getting Access token!: {str(error)}',
              '\n The error code is: {code}\n The error message is: {message}'.format(code=type(error).__name__,
                                                                                      message=str(error)))
        if error:
            raise HTTPError('There was an error in getting Access token!\\n ') from error


def get_criteo_data(access_token: str):
    uri = 'https://api.criteo.com/2022-01/statistics/report'
    date_type = '%Y-%m-%d'
    yesterday = (datetime.now() - timedelta(days=1)).strftime(date_type)

    payload = json.dumps({
        "advertiserIds": "64410",
        "startDate": "2021-01-01",
        "endDate": f'{yesterday}',
        "format": "csv",
        "dimensions": [
            "Day",
            "AdsetId",
            "Adset",
            "advertiserId",
            "advertiser",
            "CategoryId",
            "CampaignId",
            "Campaign",
            "Device"
        ],
        "metrics": [
            "Displays",
            "Clicks",
            "AdvertiserCost",
            "RevenueGeneratedClientAttribution",
            "SalesClientAttribution",
            "SalesAllClientAttribution",
            "AdvertiserValue",
            "AdvertiserAllValue"
        ],
        "timezone": "EST",
        "currency": "USD"
    })
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    try:
        response = session.post(url=uri,
                                headers=headers,
                                data=payload,
                                verify=True,
                                timeout=600 * 60,
                                stream=True).text
        print('Success! Data was received.')

        return response

    except Exception as error:
        print(f'There was an error in getting Data! {str(error)}')


"""
 Get bq data in order to get the latest date
"""
client = bigquery.Client()


def create_table():
    # query the table
    return client.query(
        """
        CREATE OR REPLACE TABLE
          `ex-nuts.xplenty.criteo_data_1` AS
        SELECT
           *
        FROM
          `ex-nuts.xplenty.criteo_data_1_staging`;
        """
    )


def delete_data_from_temp_table():
    # delete all rows from temp table
    return client.query(
        """
        DELETE
    FROM
      `ex-nuts.xplenty.criteo_data_1_staging`
    WHERE
      TRUE;
        """
    )


def run_task():
    project_id = "ex-nuts"
    dataset_name = 'xplenty'
    table_name = 'criteo_data_1'

    table_id = f"{project_id}.{dataset_name}.{table_name}_staging"

    # Create Table Schema
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True
    )

    column_names = [
        'date',
        'adset_id',
        'adset_name',
        'advertiser_id',
        'advertiser_name',
        'category_id',
        'category_name',
        'campaign_id',
        'campaign_name',
        'device',
        'currency',
        'displays',
        'clicks',
        'cost',
        'revenue',
        'sales',
        'all_sales',
        'value',
        'all_value'

    ]

    data_types = {
        'date': 'string',
        'adset_id': 'string',
        'adset_name': 'string',
        'advertiser_id': 'string',
        'advertiser_name': 'string',
        'category_id': 'string',
        'category_name': 'string',
        'campaign_id': 'string',
        'campaign_name': 'string',
        'device': 'string',
        'currency': 'string',
        'displays': 'float64',
        'clicks': 'float64',
        'cost': 'float64',
        'revenue': 'float64',
        'sales': 'float64',
        'all_sales': 'float64',
        'value': 'float64',
        'all_value': 'float64'
    }

    delete_data_from_temp_table()

    rows = 100001

    dask_data = get_criteo_data(access_token=get_criteo_access_token(client_id=CLIENT_ID,
                                                                     client_secret=CLIENT_SECRET))
    with open(path, 'w') as file:
        file.write(dask_data)

        file.close()

    dask_data = dd.read_csv(path, sep=';', names=column_names, header=0, dtype=data_types, ).head(n=rows)

    print('started to push data into bq')

    job = client.load_table_from_dataframe(
        dask_data, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    create_table()
    return print('done')


run_task()
