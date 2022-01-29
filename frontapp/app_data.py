from google.cloud import storage
import pandas as pd

def read_gcs() -> pd.DataFrame:
    """
    read dataframe from parquet file in gcs
    :return:
    """
    bucket_name = "crime-stat-app-us"
    file_path = f"gs://{bucket_name}/front/dashboard.parquet"
    return pd.read_parquet(file_path).sort_values(by="datetime", ascending=False)


def get_token():
    client = storage.Client()
    bucket_name = "crime-stat-app-us"
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob("mapbox.txt")
    return blob.download_as_string().decode("utf-8").strip()

mapbox_access_token = get_token()
df = read_gcs()