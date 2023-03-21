from __future__ import annotations
from typing import Final
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, Client, LoadJob, Table
import dask.dataframe as dd

client = bigquery.Client()


def upload_data_to_bq(project_id: str,
                            dataset_name: str,
                            table_name: str,
                            data_to_send: dd.DataFrame,
                            data_type: str) -> None:
    global client
    client = bigquery.Client(project_id)
    table_id: Final[str] = f"{project_id}.{dataset_name}.{table_name}_staging"

    if data_type == 'session':
        schemas = [
            bigquery.SchemaField("client_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("dateHourMinute", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("ga_source_medium", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("ga_adwordscampaign", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("sessions", bigquery.enums.SqlTypeNames.FLOAT64),

            # bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.STRING),
            # bigquery.SchemaField("account_name", bigquery.enums.SqlTypeNames.STRING),
            # bigquery.SchemaField("ga_sourceMedium", bigquery.enums.SqlTypeNames.STRING),
            # bigquery.SchemaField("ga_adwordsCampaignID", bigquery.enums.SqlTypeNames.STRING),
            # bigquery.SchemaField("ga_campaign", bigquery.enums.SqlTypeNames.STRING),
            # bigquery.SchemaField("ga_goal14Completions", bigquery.enums.SqlTypeNames.FLOAT64),
            # bigquery.SchemaField("ga_goal3Completions", bigquery.enums.SqlTypeNames.FLOAT64),
            # bigquery.SchemaField("ga_newUsers", bigquery.enums.SqlTypeNames.FLOAT64),
            # bigquery.SchemaField("ga_sessions", bigquery.enums.SqlTypeNames.FLOAT64),
        ]
    elif data_type == 'transaction':
        schemas = [
            bigquery.SchemaField("client_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("dateHourMinute", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("ga_source_medium", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("ga_adwordscampaign", bigquery.enums.SqlTypeNames.STRING),
            # bigquery.SchemaField("ga_campaign", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("ga_transactionId", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("transactions", bigquery.enums.SqlTypeNames.FLOAT64),

            # bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.STRING),
            # bigquery.SchemaField("account_name", bigquery.enums.SqlTypeNames.STRING),
            # bigquery.SchemaField("ga_sourceMedium", bigquery.enums.SqlTypeNames.STRING),
            # bigquery.SchemaField("ga_campaign", bigquery.enums.SqlTypeNames.STRING),
            # bigquery.SchemaField("ga_transactionId", bigquery.enums.SqlTypeNames.STRING),
            # bigquery.SchemaField("ga_transactions", bigquery.enums.SqlTypeNames.FLOAT64),
            # bigquery.SchemaField("ga_transactionRevenue", bigquery.enums.SqlTypeNames.FLOAT64),
        ]
    else:
        raise ValueError(f"Invalid data_type: {data_type}. data_type should be either session or transaction")

    print(f'Pushing to bq to {table_id}')
    # Create Table Schema
    job_config: Final[LoadJobConfig] = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        skip_leading_rows=0,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=False,
        schema=schemas
    )
    job: Final[LoadJob] = client.load_table_from_dataframe(
        data_to_send, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table: Final[Table] = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")