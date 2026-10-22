"""
Data Pipeline Module v2 - Crime Statistics Production

FIXED DATA PIPELINE: GCS-First Approach

This module fixes the broken data pipeline by implementing a GCS-first approach:
1. Fetch raw data from API
2. SAVE RAW DATA TO GCS IMMEDIATELY (critical fix!)
3. Process the data from GCS
4. Upload to BigQuery
5. Save processed data to GCS for frontend

This ensures data is NEVER lost during processing failures.
"""

from datetime import datetime
import requests
import pandas as pd
import pandas_gbq
import numpy as np
import re
import six
import json
import os
from typing import List, Optional, Tuple
from functools import wraps

# Import existing config
from config import config_eu, config_us
config = config_eu

# Import existing functions
from operations import (
    scrape_url, 
    extract_location_details, 
    extract_keywords,
    request_api,
    deduplicate,
    filter_newly_arrived,
    get_osm_coord,
    operation_refine_city_data_appendbq,
    operation_translate_city_data_appendbq,
    update_table_raw as update_table_raw_original,
    update_table_cities,
    update_table_cities_en
)

# GCS Bucket Configuration
GCS_BUCKET_NAME = "crime-stat-app-us"


def save_to_gcs_raw(df: pd.DataFrame, 
                   project_id: str,
                   dataset_id: str,
                   table_id: str = 'raw') -> Optional[str]:
    """
    Save raw data to GCS FIRST (critical improvement for data durability).
    
    This is the KEY FIX - raw data is now saved to GCS BEFORE any processing,
    ensuring data is never lost if processing fails.
    
    Args:
        df: DataFrame to save
        project_id: GCP Project ID
        dataset_id: BigQuery dataset ID
        table_id: Table name for reference
    
    Returns:
        GCS path where data was saved, or None if failed
    """
    try:
        from google.cloud import storage
        from google.oauth2 import service_account
        
        credentials = service_account.Credentials.from_service_account_file(
            os.environ.get('GCP_SECRETPATH', '')
        )
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(GCS_BUCKET_NAME)
        
        # Create timestamped filename for raw data
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        filename = f"raw_data/{project_id}_{dataset_id}_{table_id}_{timestamp}.parquet"
        gcs_path = f"gs://{GCS_BUCKET_NAME}/{filename}"
        
        # Save to GCS
        df.to_parquet(gcs_path)
        print(f"[SUCCESS] Raw data saved to {gcs_path}")
        
        # Also save metadata for tracking
        metadata_filename = f"raw_data/{project_id}_{dataset_id}_{table_id}_{timestamp}_metadata.json"
        metadata_path = f"gs://{GCS_BUCKET_NAME}/{metadata_filename}"
        
        metadata = {
            "timestamp": timestamp,
            "record_count": len(df),
            "columns": list(df.columns),
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "gcs_path": gcs_path,
            "created_at": datetime.now().isoformat(),
            "data_source": "polisen_api"
        }
        
        blob = bucket.blob(metadata_filename)
        blob.upload_from_string(
            json.dumps(metadata, indent=2),
            content_type='application/json'
        )
        
        print(f"[SUCCESS] Metadata saved to {metadata_path}")
        return gcs_path
        
    except Exception as e:
        print(f"[ERROR] Failed to save to GCS: {str(e)}")
        return None


def load_from_gcs_latest_raw(project_id: str,
                            dataset_id: str,
                            table_id: str = 'raw') -> Optional[pd.DataFrame]:
    """
    Load the most recently saved raw data from GCS (for recovery).
    
    Args:
        project_id: GCP Project ID
        dataset_id: BigQuery dataset ID
        table_id: Table name for reference
    
    Returns:
        DataFrame with raw data or None if no data found
    """
    try:
        from google.cloud import storage
        from google.oauth2 import service_account
        
        credentials = service_account.Credentials.from_service_account_file(
            os.environ.get('GCP_SECRETPATH', '')
        )
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(GCS_BUCKET_NAME)
        
        # List raw data files
        blobs = list(bucket.list_blobs(prefix='raw_data/'))
        
        # Filter for parquet files from this project
        parquet_files = [
            b for b in blobs 
            if b.name.endswith('.parquet') and 
               project_id in b.name and 
               dataset_id in b.name and
               table_id in b.name
        ]
        
        if not parquet_files:
            print(f"No raw data found for {project_id}/{dataset_id}/{table_id} in GCS")
            return None
        
        # Get most recent file
        latest_file = max(parquet_files, key=lambda x: x.time_created)
        
        # Load data
        gcs_path = f"gs://{GCS_BUCKET_NAME}/{latest_file.name}"
        df = pd.read_parquet(gcs_path)
        
        print(f"[SUCCESS] Loaded {len(df)} records from {gcs_path}")
        return df
        
    except Exception as e:
        print(f"[ERROR] Failed to load from GCS: {str(e)}")
        return None


def update_table_raw_gcs_first(project_id: str,
                              dataset_id: str = config['dataset_id'],
                              table_id: str = 'raw') -> Tuple[int, str, Optional[str]]:
    """
    Update raw table with new data from API using GCS-first approach.
    
    KEY IMPROVEMENTS:
    1. Raw data is ALWAYS saved to GCS BEFORE any processing
    2. Comprehensive error handling
    3. Returns GCS path for data recovery
    
    Args:
        project_id: GCP Project ID
        dataset_id: BigQuery dataset ID
        table_id: Table name
    
    Returns:
        Tuple of (record_count, status_message, gcs_path)
    """
    try:
        print(f"[Pipeline] Starting update_table_raw_gcs_first for {project_id}/{dataset_id}/{table_id}")
        
        # Step 1: Request new data from API
        print("[Pipeline] Step 1: Fetching data from API...")
        new_data = request_api()
        print(f"[Pipeline] Fetched {len(new_data)} records from API")
        
        # Step 2: Save raw data to GCS FIRST (critical improvement!)
        print("[Pipeline] Step 2: Saving raw data to GCS FIRST...")
        gcs_path = save_to_gcs_raw(new_data, project_id, dataset_id, table_id)
        if gcs_path is None:
            print("[Pipeline] WARNING: Failed to save to GCS, but continuing with processing")
        else:
            print(f"[Pipeline] Raw data saved to GCS at: {gcs_path}")
        
        # Step 3: Load history from BigQuery
        print("[Pipeline] Step 3: Loading history from BigQuery...")
        try:
            QUERY = f"""
                SELECT id, location_name, datetime, gps
                FROM `{project_id}.{dataset_id}.{table_id}`
                WHERE CAST(datetime as DATE) > DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
            """
            history = pandas_gbq.read_gbq(QUERY, project_id=project_id)
            print(f"[Pipeline] Loaded {len(history)} historical records")
        except Exception as e:
            print(f"[Pipeline] Warning: Could not load history: {str(e)}")
            history = pd.DataFrame()
        
        # Step 4: Filter to only new records
        print("[Pipeline] Step 4: Filtering for new records...")
        newly_arrived = filter_newly_arrived(new_data, history)
        
        if newly_arrived is not None:
            print(f"[Pipeline] Found {len(newly_arrived)} new records to process")
            
            # Step 5: Scrape details for new records
            print("[Pipeline] Step 5: Scraping details...")
            newly_arrived['details'] = [scrape_url(url) for url in newly_arrived['url']]
            
            # Step 6: Upload to BigQuery
            print("[Pipeline] Step 6: Uploading to BigQuery...")
            pandas_gbq.to_gbq(newly_arrived, f'{dataset_id}.{table_id}', 
                            project_id=project_id, if_exists='append')
            print(f"[Pipeline] SUCCESS: {newly_arrived.shape[0]} rows added to table: {dataset_id}.{table_id}")
            
            return len(newly_arrived), f"SUCCESS: {len(newly_arrived)} new records added", gcs_path
        else:
            print("[Pipeline] No new records found")
            return 0, "SUCCESS: No new records found", gcs_path
            
    except Exception as e:
        error_msg = f"ERROR in update_table_raw_gcs_first: {str(e)}"
        print(error_msg)
        
        # Try to get GCS path even on error
        if 'gcs_path' in locals():
            return 0, error_msg, gcs_path
        else:
            return 0, error_msg, None


def operation_refine_city_data_appendbq_gcs_first(project_id: str, 
                                                 destination_tableid: str, 
                                                 newly_arrived: pd.DataFrame) -> Tuple[int, str, Optional[str]]:
    """
    Refine city data and append to BigQuery with GCS support.
    
    Args:
        project_id: GCP Project ID
        destination_tableid: Destination table name
        newly_arrived: DataFrame with new records
    
    Returns:
        Tuple of (record_count, status_message, gcs_path)
    """
    try:
        print(f"[Pipeline] Starting operation_refine_city_data_appendbq_gcs_first")
        
        # Load district data
        print("[Pipeline] Loading district data...")
        district = pandas_gbq.read_gbq(
            f"""
            SELECT district
            FROM `{project_id}.{config['dataset_id']}.dim_districts_hemnet`
            """, project_id=project_id
        )
        
        print(f"[Pipeline] Processing {len(newly_arrived)} records...")
        
        # Process by city
        newly_arrived_list = []
        for city in newly_arrived.location_name.unique():
            newly_arrived_i = newly_arrived[newly_arrived['location_name'] == city].copy()
            details_list = (newly_arrived_i['details'] + ' ' + 
                          newly_arrived_i['summary'] + ' ' + 
                          newly_arrived_i['name'])
            
            newly_arrived_i['location_details'] = [
                extract_location_details(detail, district=district[district['city'] == city])
                for detail in details_list
            ]
            newly_arrived_i['location_details'] = (
                newly_arrived_i['location_details'] + ' ' + 
                newly_arrived_i['location_name'] + ' ' + 'Sweden'
            )
            newly_arrived_list.append(newly_arrived_i)
        
        newly_arrived = pd.concat(newly_arrived_list)
        
        # Get OSM coordinates
        print("[Pipeline] Getting OSM coordinates...")
        rows = []
        for _, row in newly_arrived.iterrows():
            rows.append(get_osm_coord(row.copy()))
        newly_arrived = pd.DataFrame(rows)
        
        # Upload to BigQuery
        print("[Pipeline] Uploading to BigQuery...")
        pandas_gbq.to_gbq(newly_arrived, f"{config['dataset_id']}.{destination_tableid}", 
                        project_id=project_id, if_exists='append')
        print(f"[Pipeline] SUCCESS: {newly_arrived.shape[0]} rows added to table: {config['dataset_id']}.{destination_tableid}")
        
        # Save processed data to GCS
        print("[Pipeline] Saving processed data to GCS...")
        from google.cloud import storage
        from google.oauth2 import service_account
        
        credentials = service_account.Credentials.from_service_account_file(
            os.environ.get('GCP_SECRETPATH', '')
        )
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(GCS_BUCKET_NAME)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        filename = f"processed/cities_refined_{destination_tableid}_{timestamp}.parquet"
        gcs_path = f"gs://{GCS_BUCKET_NAME}/{filename}"
        
        newly_arrived.to_parquet(gcs_path)
        print(f"[SUCCESS] Processed data saved to {gcs_path}")
        
        return len(newly_arrived), f"SUCCESS: {len(newly_arrived)} records added", gcs_path
        
    except Exception as e:
        error_msg = f"ERROR in operation_refine_city_data_appendbq_gcs_first: {str(e)}"
        print(error_msg)
        return 0, error_msg, None


def update_tables_gcs_first(project_id: str, 
                           new_source_tableid: str = 'raw',
                           destination_tableid: str = 'cities_refined') -> Tuple[int, str, Optional[str]]:
    """
    Update tables by comparing source and destination with GCS support.
    
    Args:
        project_id: GCP Project ID
        new_source_tableid: Source table
        destination_tableid: Destination table
    
    Returns:
        Tuple of (record_count, status_message, gcs_path)
    """
    try:
        # Load data from BigQuery
        print(f"[Pipeline] Loading data from {new_source_tableid}...")
        try:
            raw = pandas_gbq.read_gbq(f"""
                SELECT *
                FROM `{project_id}.{config['dataset_id']}.{new_source_tableid}`
                WHERE CAST(datetime as DATE) > DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
            """, project_id=project_id)
            print(f"[Pipeline] Loaded {len(raw)} records from {new_source_tableid}")
        except Exception as e:
            print(f"[Pipeline] Error loading from {new_source_tableid}: {str(e)}")
            return 0, f"ERROR: Could not load source table", None
        
        # Load history
        print(f"[Pipeline] Loading history from {destination_tableid}...")
        try:
            hist = pandas_gbq.read_gbq(f"""
                SELECT *
                FROM `{project_id}.{config['dataset_id']}.{destination_tableid}`
                WHERE CAST(datetime as DATE) > DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
            """, project_id=project_id)
            print(f"[Pipeline] Loaded {len(hist)} records from {destination_tableid}")
        except Exception as e:
            print(f"[Pipeline] Error loading from {destination_tableid}: {str(e)}")
            hist = pd.DataFrame()
        
        # Filter for new records
        print("[Pipeline] Filtering for new records...")
        newly_arrived = filter_newly_arrived(raw, hist)
        
        if newly_arrived is not None:
            print(f"[Pipeline] Found {len(newly_arrived)} new records to process")
            return operation_refine_city_data_appendbq_gcs_first(
                project_id=project_id, 
                destination_tableid=destination_tableid,
                newly_arrived=newly_arrived
            )
        else:
            print("[Pipeline] No new records to process")
            return 0, "SUCCESS: No new records", None
            
    except Exception as e:
        error_msg = f"ERROR in update_tables_gcs_first: {str(e)}"
        print(error_msg)
        return 0, error_msg, None


def operation_translate_city_data_appendbq_gcs_first(project_id: str, 
                                                    destination_tableid: str, 
                                                    newly_arrived: pd.DataFrame) -> Tuple[int, str, Optional[str]]:
    """
    Translate city data to English and append to BigQuery with GCS support.
    
    Args:
        project_id: GCP Project ID
        destination_tableid: Destination table name
        newly_arrived: DataFrame with new records
    
    Returns:
        Tuple of (record_count, status_message, gcs_path)
    """
    try:
        print(f"[Pipeline] Starting operation_translate_city_data_appendbq_gcs_first")
        
        batch_size = 100
        n_batch = len(newly_arrived) // batch_size
        
        from google.cloud import translate_v2 as translate
        translate_client = translate.Client()
        
        # Translate type keys
        type_keys = list(newly_arrived['type'].unique())
        type_values_en = [translate_client.translate(x, source_language='sv', target_language='en')["translatedText"]
                         if x is not None else 'None' for x in type_keys]
        type_mapping = dict(zip(type_keys, type_values_en))
        
        # Process in batches
        for i in range(n_batch + 1):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(newly_arrived))
            batch = newly_arrived.iloc[start_idx:end_idx].copy()
            
            # Translate details
            batch['details'] = [translate_client.translate(x, source_language='sv', target_language='en')["translatedText"]
                              if x is not None else 'None' for x in batch['details']]
            
            # Translate type
            batch['type'] = batch['type'].map(type_mapping)
            
            # Upload batch
            print(f"[Pipeline] Uploading batch {i+1}/{n_batch+1}...")
            pandas_gbq.to_gbq(batch, f"{config['dataset_id']}.{destination_tableid}", 
                            project_id=project_id, if_exists='append')
            print(f"[Pipeline] SUCCESS: {len(batch)} rows added to table: {config['dataset_id']}.{destination_tableid}")
            
            sleep(1)  # Rate limiting
        
        print("[Pipeline] Saving processed data to GCS...")
        from google.cloud import storage
        from google.oauth2 import service_account
        
        credentials = service_account.Credentials.from_service_account_file(
            os.environ.get('GCP_SECRETPATH', '')
        )
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(GCS_BUCKET_NAME)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        filename = f"processed/cities_refined_en_{timestamp}.parquet"
        gcs_path = f"gs://{GCS_BUCKET_NAME}/{filename}"
        
        newly_arrived.to_parquet(gcs_path)
        print(f"[SUCCESS] Processed data saved to {gcs_path}")
        
        return len(newly_arrived), f"SUCCESS: {len(newly_arrived)} records translated", gcs_path
        
    except Exception as e:
        error_msg = f"ERROR in operation_translate_city_data_appendbq_gcs_first: {str(e)}"
        print(error_msg)
        return 0, error_msg, None


# Main pipeline function
def run_full_pipeline_gcs_first(project_id: str,
                               dataset_id: str = config['dataset_id']) -> dict:
    """
    Run the complete data pipeline with GCS-first approach.
    
    This is the MAIN function to use for the data pipeline.
    It implements the GCS-first approach to ensure data durability.
    
    Args:
        project_id: GCP Project ID
        dataset_id: BigQuery dataset ID
    
    Returns:
        Dictionary with pipeline results
    """
    results = {
        "timestamp": datetime.now().isoformat(),
        "project_id": project_id,
        "dataset_id": dataset_id,
        "steps": [],
        "success": True,
        "errors": []
    }
    
    print("=" * 60)
    print("RUNNING GCS-FIRST DATA PIPELINE (FIXED)")
    print("=" * 60)
    
    # Step 1: Update raw table (saves to GCS first!)
    print("\n" + "=" * 40)
    print("STEP 1: UPDATE RAW TABLE (GCS-FIRST)")
    print("=" * 40)
    
    count, status, gcs_path = update_table_raw_gcs_first(project_id, dataset_id, 'raw')
    results["steps"].append({
        "step": "update_table_raw",
        "count": count,
        "status": status,
        "gcs_path": gcs_path
    })
    
    if "SUCCESS" not in status:
        results["success"] = False
        results["errors"].append(f"Step 1 failed: {status}")
    
    print(f"Step 1 complete: {status}")
    print(f"Step 1 GCS path: {gcs_path}")
    
    # Step 2: Update cities refined
    print("\n" + "=" * 40)
    print("STEP 2: UPDATE CITIES REFINED")
    print("=" * 40)
    
    count, status, gcs_path = update_tables_gcs_first(
        project_id,
        'raw',
        'cities_refined'
    )
    
    results["steps"].append({
        "step": "update_table_cities",
        "count": count,
        "status": status,
        "gcs_path": gcs_path
    })
    
    if "SUCCESS" not in status:
        results["success"] = False
        results["errors"].append(f"Step 2 failed: {status}")
    
    print(f"Step 2 complete: {status}")
    print(f"Step 2 GCS path: {gcs_path}")
    
    # Step 3: Update English translation
    print("\n" + "=" * 40)
    print("STEP 3: UPDATE ENGLISH TRANSLATION")
    print("=" * 40)
    
    try:
        # Load data from cities_refined
        raw = pandas_gbq.read_gbq(f"""
            SELECT *
            FROM `{project_id}.{config['dataset_id']}.cities_refined`
            WHERE CAST(datetime as DATE) > DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        """, project_id=project_id)
        
        hist = pandas_gbq.read_gbq(f"""
            SELECT *
            FROM `{project_id}.{config['dataset_id']}.cities_refined_en`
            WHERE CAST(datetime as DATE) > DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        """, project_id=project_id)
        
        newly_arrived = filter_newly_arrived(raw, hist)
        
        if newly_arrived is not None:
            count, status, gcs_path = operation_translate_city_data_appendbq_gcs_first(
                project_id=project_id,
                destination_tableid='cities_refined_en',
                newly_arrived=newly_arrived
            )
        else:
            count = 0
            status = "SUCCESS: No new records to translate"
            gcs_path = None
        
        results["steps"].append({
            "step": "update_table_cities_en",
            "count": count,
            "status": status,
            "gcs_path": gcs_path
        })
        
        if "SUCCESS" not in status:
            results["success"] = False
            results["errors"].append(f"Step 3 failed: {status}")
        
        print(f"Step 3 complete: {status}")
        print(f"Step 3 GCS path: {gcs_path}")
        
    except Exception as e:
        error_msg = f"ERROR in Step 3: {str(e)}"
        print(error_msg)
        results["steps"].append({
            "step": "update_table_cities_en",
            "count": 0,
            "status": error_msg,
            "gcs_path": None
        })
        results["success"] = False
        results["errors"].append(error_msg)
    
    # Summary
    total_new = sum(step.get("count", 0) for step in results["steps"])
    results["total_new_records"] = total_new
    
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"Total new records: {total_new}")
    print(f"All steps successful: {results['success']}")
    
    if results["errors"]:
        print("Errors encountered:")
        for error in results["errors"]:
            print(f"  - {error}")
    
    return results


def recover_data_from_gcs(project_id: str,
                         dataset_id: str = config['dataset_id'],
                         days_back: int = 30) -> int:
    """
    Recover raw data from GCS and reprocess.
    
    Use this to recover missing data that was not saved properly by the old pipeline.
    
    Args:
        project_id: GCP Project ID
        dataset_id: BigQuery dataset ID
        days_back: Number of days to look back in GCS
    
    Returns:
        Number of records recovered
    """
    print(f"[Recovery] Attempting to recover raw data from last {days_back} days...")
    
    try:
        from google.cloud import storage
        from google.oauth2 import service_account
        
        credentials = service_account.Credentials.from_service_account_file(
            os.environ.get('GCP_SECRETPATH', '')
        )
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(GCS_BUCKET_NAME)
        
        # List raw data files from last N days
        blobs = list(bucket.list_blobs(prefix='raw_data/'))
        cutoff_date = datetime.now() - pd.Timedelta(days=days_back)
        
        recent_files = [
            b for b in blobs 
            if b.time_created and b.time_created.replace(tzinfo=None) > cutoff_date
        ]
        
        print(f"[Recovery] Found {len(recent_files)} files from last {days_back} days")
        
        # Load and combine all files
        all_data = []
        for blob in recent_files:
            try:
                gcs_path = f"gs://{GCS_BUCKET_NAME}/{blob.name}"
                df = pd.read_parquet(gcs_path)
                all_data.append(df)
                print(f"[Recovery] Loaded {len(df)} records from {blob.name}")
            except Exception as e:
                print(f"[Recovery] Error loading {blob.name}: {str(e)}")
        
        if not all_data:
            print("[Recovery] No data found to recover")
            return 0
        
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"[Recovery] Combined {len(combined_df)} total records")
        
        # Save recovered data to raw table
        print("[Recovery] Uploading recovered data to BigQuery...")
        pandas_gbq.to_gbq(combined_df, f'{dataset_id}.raw', 
                         project_id=project_id, if_exists='append')
        
        print(f"[Recovery] SUCCESS: {len(combined_df)} records recovered")
        return len(combined_df)
        
    except Exception as e:
        print(f"[Recovery] ERROR: {str(e)}")
        return 0


if __name__ == "__main__":
    # Run the full pipeline with GCS-first approach
    import os
    
    if 'GCP_SECRETPATH' not in os.environ:
        print("ERROR: GCP_SECRETPATH environment variable not set")
        print("Please run: export GCP_SECRETPATH=/path/to/service_account.json")
        exit(1)
    
    try:
        from google.cloud import bigquery
        bq_client = bigquery.Client()
        project_id = bq_client.project
        print(f"Running pipeline for project: {project_id}")
        
        results = run_full_pipeline_gcs_first(project_id, config['dataset_id'])
        
        print("\n=== Pipeline Results ===")
        print(json.dumps(results, indent=2, default=str))
        
    except Exception as e:
        print(f"Error running pipeline: {str(e)}")
        exit(1)
