from __future__ import annotations
from google.api_core.exceptions import NotFound
from upload_to_bq import client
import logging


def combine_data(project_id: str, dataset_name: str, table_name: str, date_field: str) -> None:
    try:
        query = f"""
            create or replace table `{project_id}.{dataset_name}.{table_name}` as
            with main as (
                select * from `{project_id}.{dataset_name}.{table_name}_staging`
            ),
            size_check as (
                select
                if(size>0,min_date,date(current_date())) as run_status
                from (
                    select count(*) as size,min(date({date_field})) as min_date
                    from main
                )
            ),
            pre_final as (
                select *
                from `{project_id}.{dataset_name}.{table_name}`
                where date({date_field})<(
                    select run_status from size_check
                )
            ),
            final as (
                select * from pre_final
                union all
                select * from main
            )
            select distinct * from final
        """
        query_job = client.query(query)
        query_job.result()
        print(f'Data was overwritten\n {project_id}.{dataset_name}.{table_name}_staging')
    except NotFound as e:
        raise ValueError(f'Table not found: {e}') from e
    except Exception as e:
        logging.error(f'Error occurred: {e}')
        raise e


def drop_table(project_id: str, dataset_name: str, table_name: str) -> None:
    try:
        query = f""" 
            drop table if exists `{project_id}.{dataset_name}.{table_name}_staging`
        """
        query_job = client.query(query)
        query_job.result()
        logging.info(f'Temp table was deleted!\n {project_id}.{dataset_name}.{table_name}_staging')
        print(f'Temp table was deleted!\n {project_id}.{dataset_name}.{table_name}_staging')
    except NotFound as e:
        raise ValueError(f'Table not found: {e}') from e
    except Exception as e:
        logging.error(f'Error occurred: {e}')
        raise e