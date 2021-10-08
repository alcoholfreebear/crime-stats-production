#ToDo:
# translate to eng from requested date= 2020-12-13 17:40:10.947178 UTC to 2020-11-14 20:50:47.142129 UTC


from datetime import datetime
import requests
import pandas as pd
import pandas_gbq
import numpy as np
import re
import six
import json
from config import config_eu, config_us
from google.oauth2 import service_account
from google.cloud import bigquery
from bs4 import BeautifulSoup
from time import sleep
from typing import List
from google.cloud import translate_v2 as translate

# choose config
config=config_eu

def set_pandas_gbq_credentials():
    credentials = service_account.Credentials.from_service_account_file(os.environ['GCP_SECRETPATH'] )
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = os.environ['GCP_PROJECID']


def update_table_raw(project_id:str,
                     dataset_id:str=config['dataset_id'],
                     table_id:str='raw'):
    """

    Args:
        project_id:
        dataset_id:
        table_id:

    Returns:

    """
    # Get history
    QUERY = f"""
            SELECT id, location_name, datetime, gps
            FROM `{project_id}.{dataset_id}.{table_id}`
            WHERE CAST(datetime as DATE) > DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
            """
    history = pandas_gbq.read_gbq(QUERY, project_id=project_id)
    # Get new data
    new_data = request_api()
    # Upload newly arrived data (id not exist in history)
    newly_arrived = filter_newly_arrived(new_data, history)
    # upload new data
    if newly_arrived is not None:
        newly_arrived['details'] = [scrape_url(url) for url in newly_arrived['url']]
        pandas_gbq.to_gbq(newly_arrived, f'{dataset_id}.{table_id}', project_id=project_id, if_exists='append')
        print(f'{newly_arrived.shape[0]} rows added to table: {dataset_id}.{table_id}')
    else:
        print(f'0 rows added to table: {dataset_id}.{table_id}')

def scrape_url(url):
    if not url.startswith('http'):
        url='https://polisen.se'+url
    sleep(0.5)
    try:
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        containers = soup.find_all('div', {'class': "text-body editorial-html"})
        return containers[0].find_all('p')[0].text.replace(u'\xa0', u' ')
    except:
        return ''

def extract_location_details(detail, district):
    detail = re.sub('[.,;!:]', ' ', str(detail))
    locwords = []
    loc_keywords = ['gata', 'vägen', 'torg', 'gärd', 'plan', 'leden', 'park' ]
    locwords.append(extract_keywords(detail, keywords = loc_keywords))
    for dist in district['district'].values:
        if dist.lower() in detail.lower():
            locwords.append(dist)
    if len(locwords)>=1:
        locword = ' '.join(np.unique(locwords))
    else:
        locword = ''
    return locword

def extract_keywords(detail, keywords):
    detail = re.sub('[.,;!:]', ' ', str(detail))
    keywords_out = []
    for keyword in keywords:
        for x in detail.lower().split(' '):
            if keyword in x:
                keywords_out.append(x)
    if len(keywords_out)>=1:
        keyword_out = ' '.join(np.unique(keywords_out))
    else:
        keyword_out = ''
    return keyword_out

def request_api():
    r = requests.get('https://polisen.se/api/events')
    df = pd.DataFrame(r.json())
    df['location_name'] = [x.get('name') for x in df['location']]
    df['gps'] = [x.get('gps') for x in df['location']]
    df['gps_lat'] = [float(x.split(',')[0]) for x in df['gps']]
    df['gps_lon'] = [float(x.split(',')[1]) for x in df['gps']]
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
    df['date_requested'] = pd.to_datetime(datetime.today(), utc=True)
    df.drop(columns=['location'], inplace=True)
    # df['details'] = [scrape_url(url) for url in df['url']]
    return df

def deduplicate(df, unique_id:str='id', sort_idx:str='datetime'):
    cols=list(df.columns)
    cols_other=list(set(cols)-set([unique_id]))
    return df.sort_values(by=sort_idx, ascending=False).groupby([unique_id])[cols_other].first().reset_index()

def filter_newly_arrived(new_data:pd.DataFrame, history:pd.DataFrame,
                         idx_cols:List[str]=['id'])->pd.DataFrame:
    """
    
    Args:
        new_data:
        history:
        idx_cols:

    Returns:
    """

    if len(new_data) >= 1:
        new_data = new_data.set_index(idx_cols)
        if len(history) >= 1:
            idx_history = history.groupby(idx_cols).count().index
        else:
            idx_history = []
        idx_add = set(new_data.index) - set(idx_history)
        if len(idx_add) >= 1:
            new_data = new_data.loc[list(idx_add)]
            new_data = new_data.reset_index()
            return deduplicate(new_data)



def upload_initial(df:pd.DataFrame, project_id:str,
                   dataset_id:str=config['dataset_id'],
                   table_id='raw'):
    pandas_gbq.to_gbq(df, f'{dataset_id}.{table_id}', project_id=project_id, if_exists='replace')


def seed_table_cities(project_id:str):
    newly_arrived = pandas_gbq.read_gbq(f"""
            SELECT *
            FROM `{project_id}.config['dataset_id'].raw`
            --WHERE location_name in (select distinct city from 
            --`{project_id}.{config['dataset_id']}.dim_district`)
            """
    )
    district = pandas_gbq.read_gbq(
            f"""
            SELECT district
            FROM `{project_id}.{config['dataset_id']}.dim_district`
            """, project_id=project_id)
    details_list = newly_arrived['details']+' '+newly_arrived['summary']+' '+ newly_arrived['name']
    newly_arrived['location_details'] = [extract_location_details(detail, district=district) for detail in details_list]
    newly_arrived['location_details'] = newly_arrived['location_details'] +' ' +newly_arrived['location_name']+' ' +'Sweden'

    upload_initial(newly_arrived, project_id=project_id,
                   dataset_id=config['dataset_id'],
                   table_id='cities_refined')


def operation_refine_city_data_appendbq_old(project_id:str, destination_tableid:str, newly_arrived: pd.DataFrame, *args, **kwargs):
    district = pandas_gbq.read_gbq(
            f"""
            SELECT distinct district
            FROM `{project_id}.{config['dataset_id']}.dim_district`
            """, project_id=project_id)

    details_list = newly_arrived['details'] + ' ' + newly_arrived['summary'] + ' ' + newly_arrived['name']
    newly_arrived['location_details'] = [extract_location_details(detail, district=district) for detail in details_list]
    newly_arrived['location_details'] = newly_arrived['location_details'] + ' ' + newly_arrived['location_name'] + ' ' + 'Sweden'
    pandas_gbq.to_gbq(newly_arrived, f"{config['dataset_id']}.{destination_tableid}", project_id=project_id, if_exists='append')
    print(f"{newly_arrived.shape[0]} rows added to table: {config['dataset_id']}.{destination_tableid}")




def osm_api_url(search_term):
    return f"https://nominatim.openstreetmap.org/search/405 {search_term}?format=json&limit=1"

def get_osm_coord(row):
    if 'västmanlands' in row['location_name'].lower():
        return {**row, 'osm_lon':row['gps_lon'], 'osm_lat':row['gps_lat']}
    result = requests.get(osm_api_url(row['location_details'])).text
    r_js=json.loads(result)
    if len(r_js)>0:
        r_js=r_js[0]
        return {**row, 'osm_lon':float(r_js['lon']), 'osm_lat':float(r_js['lat'])}
    else:
        return {**row, 'osm_lon':row['gps_lon'], 'osm_lat':row['gps_lat']}


def operation_refine_city_data_appendbq(project_id:str, destination_tableid:str, newly_arrived: pd.DataFrame, *args, **kwargs):
    district = pandas_gbq.read_gbq(
        f"""
        SELECT * FROM `{project_id}.{config['dataset_id']}.dim_districts_hemnet`
        """, project_id=project_id)

    print(newly_arrived.shape)
    newly_arrived_list=[]
    for city in newly_arrived.location_name.unique():
        newly_arrived_i=newly_arrived[newly_arrived['location_name']==city].copy()
        details_list = newly_arrived_i['details']+' '+newly_arrived_i['summary']+' '+ newly_arrived_i['name']
        newly_arrived_i['location_details'] = [extract_location_details(detail, district=district[district['city']==city]) for detail in details_list]
        newly_arrived_i['location_details'] = newly_arrived_i['location_details'] +' ' +newly_arrived_i['location_name']+' ' +'Sweden'
        newly_arrived_list.append(newly_arrived_i)
    newly_arrived=pd.concat(newly_arrived_list)
    # get osm coordinates
    rows=[]
    for _, row in newly_arrived.iterrows():
        rows.append(get_osm_coord(row.copy()))
    newly_arrived=pd.DataFrame(rows)
    pandas_gbq.to_gbq(newly_arrived, f"{config['dataset_id']}.{destination_tableid}", project_id=project_id,
                      if_exists='append')
    print(f"{newly_arrived.shape[0]} rows added to table: {config['dataset_id']}.{destination_tableid}")


def update_tables(project_id: str, new_source_tableid:str='raw'
                        , destination_tableid:str='cities_refined'
                        , operation_func=operation_refine_city_data_appendbq):
    raw = pandas_gbq.read_gbq(f"""
            SELECT *
            FROM `{project_id}.{config['dataset_id']}.{new_source_tableid}`
            WHERE CAST(datetime as DATE) > DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
            """
    )
    hist = pandas_gbq.read_gbq(f"""
            SELECT *
            FROM `{project_id}.{config['dataset_id']}.{destination_tableid}`
            WHERE CAST(datetime as DATE) > DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
            """
    )
    newly_arrived = filter_newly_arrived(raw, hist)
    if newly_arrived is not None:
        operation_func(project_id=project_id, destination_tableid=destination_tableid
                                       , newly_arrived=newly_arrived)
    else:
        print(f"0 rows added to table: {config['dataset_id']}.{destination_tableid}")


def decode_text(text):
    if isinstance(text, six.binary_type):
        text = text.decode("utf-8")
    return text

def translate_text_googleapis(text):
    translate_client = translate.Client()
    result = translate_client.translate(text, source_language='sv',  target_language='en')
    return result["translatedText"]

def translate_text_pytrans(text_arr):
    from googletrans import Translator
    translator = Translator()
    return translator.translate(text_arr, src='sv', dest='en').text


def operation_translate_city_data_appendbq(project_id:str, destination_tableid:str, newly_arrived:pd.DataFrame, *args, **kwargs):
    batch_size = 100
    n_batch = len(newly_arrived) // batch_size
    dfs = np.array_split(newly_arrived, n_batch+1)
    for df in dfs:
        translate_client = translate.Client()
        df['details'] = [decode_text(x) for x in df['details']]
        df['details'] = [translate_client.translate(x, source_language='sv',  target_language='en')["translatedText"]
                                if x is not None else 'None' for x in df['details']]
        type_keys = list(df['type'].unique())
        type_values_en = [translate_client.translate(x, source_language='sv', target_language='en')["translatedText"]
                          if x is not None else 'None' for x in type_keys]
        type_mapping = dict(zip(type_keys, type_values_en))
        df['type'] = df['type'].map(type_mapping)
        pandas_gbq.to_gbq(df, f"{config['dataset_id']}.{destination_tableid}",
                          project_id=project_id, if_exists='append')
        print(f"{df.shape[0]} rows added to table: {config['dataset_id']}.{destination_tableid}")
        sleep_time=1
        print(f"sleeping {sleep_time}")
        sleep(sleep_time)
    print(f"{newly_arrived.shape[0]} rows added to table: {config['dataset_id']}.{destination_tableid}")


def update_table_cities(project_id: str):
    update_tables(project_id=project_id, new_source_tableid='raw'
                    , destination_tableid='cities_refined'
                    , operation_func=operation_refine_city_data_appendbq)

def update_table_cities_en(project_id: str):
    update_tables(project_id=project_id, new_source_tableid='cities_refined'
                    , destination_tableid='cities_refined_en'
                    , operation_func=operation_translate_city_data_appendbq)


def get_dim_district(project_id):
    uppsala_list = ['Fjärdingen', 'Berthåga', 'Husbyborg', 'Hällby', 'Librobäck',
                    'Luthagen', 'Rickomberga', 'Stenhagen', 'Eriksberg', 'Flogsta',
                    'Ekeby', 'Håga', 'Kvarnbo', 'Kåbo', 'Norby', 'Polacksbacken',
                    'Starbo', 'Gottsunda', 'Sunnersta', 'Ulleråker', 'Ultuna',
                    'Valsätra', 'Vårdsätra', 'Bergsbrunna', 'Danmark-Säby', 'Nåntuna',
                    'Sävja', 'Vilan', 'Boländerna', 'Fyrislund', 'Fålhagen',
                    'Kungsängen', 'Kuggebro', 'Sala backe', 'Slavsta', 'Vaksala',
                    'Årsta', 'Brillinge', 'Gamla Uppsala', 'Gränby', 'Kvarngärdet',
                    'Löten', 'Nyby', 'Svartbäcken', 'Tunabackar', 'Ärna', 'Storvreta',
                    'Rasbo', 'Centrum', 'Skuttunge', 'Skyttorp', 'Tycho Hedéns väg']

    stockholm_list = ['Bromma', 'Enskede', 'Årsta', 'Vantörs', 'Farsta', 'Hägersten', 'Älvsjö', 'Hässelby', 'Vällingby', 'Kungsholmens',
                      'Norrmalms', 'Rinkeby', 'Kista', 'Skarpnäcks', 'Skärholmens', 'Spånga', 'Tensta', 'Södermalms', 'Östermalms', 'Täby', 'Solna', 'Sundbyberg']

    gavle_list = ['Alderholmen', 'Andersberg', 'Bomhus', 'Brynäs', 'Fredriksskans', 'Fridhem', 'Järvsta', 'Gamla Gävle',
                  'Hagaström', 'Hemlingby', 'Hemsta', 'Hille', 'Varva', 'Höjersdal', 'Lexe', 'Nordost', 'Norr', 'Norrtull',
                  'Nynäs', 'Näringen', 'Olsbacka', 'Stigslund', 'Strömsbro', 'Sätra', 'Söder',
                  'Södertull', 'Sörby', 'Sörby urfjäll', 'Vall', 'Vallbacken', 'Villastaden', 'Väster', 'Tolvfors', 'Åbyggeby', 'Öster']
    karlskrona_list = ['Aspö', 'Augerum', 'Flymen', 'Fridlevstad', 'Hasslö', 'Jämjö', 'Karlskrona', 'Kristianopel', 'Lösen',
                       'Nättraby', 'Ramdala', 'Rödeby', 'Sillhövda', 'Sturkö', 'Torhamn', 'Tving']

    dfs = [city_district('Uppsala', uppsala_list),
           city_district('Stockholm', stockholm_list),
           city_district('Gävle', gavle_list),
           city_district('Karlskrona', karlskrona_list)
           ]
    return pd.concat(dfs)

def seed_dim_district(project_id):
    dim_district = get_dim_district()
    upload_initial(dim_district, project_id=project_id,
                   dataset_id=config['dataset_id'],
                   table_id='dim_district')

def save_to_gcs(project_id:str):
    """
    :param project_id:
    :return:
    """
    df = pandas_gbq.read_gbq(f"""SELECT * FROM `{project_id}.crime_statistics.dashboard` 
                        """, project_id=project_id)
    df['date_requested'] = pd.to_datetime(df['date_requested']).dt.strftime('%Y-%m-%d %H:%M')
    df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d %H:%M')
    df['date_requested'] = pd.to_datetime(df['date_requested'])
    df['datetime'] = pd.to_datetime(df['datetime'])
    bucket_name='crime-stat-app-us'
    file_path=f'gs://{bucket_name}/front/dashboard.parquet'
    df.to_parquet(file_path)
    print(f'parquet file saved to {file_path}')


def main():
    bq_client = bigquery.Client()
    project_id = bq_client.project
    # set_pandas_gbq_credentials()
    update_table_raw(project_id=project_id,  dataset_id=config['dataset_id'], table_id='raw')
    update_table_cities(project_id)
    # translate
    update_table_cities_en(project_id)
    save_to_gcs(project_id)

def translate_ops():
    bq_client = bigquery.Client()
    project_id = bq_client.project
    update_table_cities_en(project_id)






