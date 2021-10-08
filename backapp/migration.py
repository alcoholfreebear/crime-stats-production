
from datetime import datetime
import requests
import pandas as pd
import pandas_gbq
from config import config_eu, config_us
from google.cloud import bigquery
from typing import List


def operation_refine_city_data_appendbq(project_id:str, destination_tableid:str, newly_arrived: pd.DataFrame, *args, **kwargs):
    district = pandas_gbq.read_gbq(
            f"""
            SELECT district
            FROM `{project_id}.crime_statistics_polisenapi.dim_district`
            """, project_id=project_id)

    details_list = newly_arrived['details'] + ' ' + newly_arrived['summary'] + ' ' + newly_arrived['name']
    newly_arrived['location_details'] = [extract_location_details(detail, district=district) for detail in details_list]
    newly_arrived['location_details'] = newly_arrived['location_details'] + ' ' + newly_arrived['location_name'] + ' ' + 'Sweden'
    pandas_gbq.to_gbq(newly_arrived, f'crime_statistics_polisenapi.{destination_tableid}', project_id=project_id, if_exists='append')
    print(f'{newly_arrived.shape[0]} rows added to table: crime_statistics_polisenapi.{destination_tableid}')


def move_tables(project_id: str, config_source: dict=config_us, config_destination: dict=config_eu,
                table_ids:List[str]=['raw', 'cities_refined_en', 'cities_refined','dim_district' ]):
    source_dataset_id = config_source['dataset_id']
    destination_dataset_id = config_destination['dataset_id']
    for table_id in table_ids:
        df = pandas_gbq.read_gbq(
            f"""
                SELECT  * 
                FROM `{project_id}.{source_dataset_id}.{table_id}`
                """)
        pandas_gbq.to_gbq(df, f'{destination_dataset_id}.{table_id}', project_id=project_id, location=config_destination['location'],
                          if_exists='replace')
def main():
    bq_client = bigquery.Client()
    project_id = bq_client.project
    move_tables(project_id=project_id)

if __name__=='__main__':
    main()






