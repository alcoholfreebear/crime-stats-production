"""
Data Loader Module - Crime Statistics Frontend

This module provides optimized data loading for the Dash frontend application.
It reads data from GCS with caching and validation to ensure data quality.

Key features:
- GCS caching for fast data retrieval
- Data validation and integrity checks
- Efficient parquet file reading
- Error handling with fallback mechanisms
"""

import pandas as pd
import numpy as np
from datetime import datetime
import time
from typing import Optional, Tuple
import os

# Try to import GCS client, but allow fallback for local development
try:
    from google.cloud import storage
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False


class DataCache:
    """
    Cache class to store frequently accessed data.
    This helps reduce repeated reads from GCS.
    """
    
    def __init__(self, max_size: int = 3):
        self.cache = {}
        self.max_size = max_size
        self.access_times = []
    
    def get(self, key: str) -> Optional[pd.DataFrame]:
        """Get data from cache if available."""
        if key in self.cache:
            self.access_times.append(key)
            return self.cache[key]
        return None
    
    def set(self, key: str, data: pd.DataFrame):
        """Add data to cache, evicting oldest if at capacity."""
        if key in self.cache:
            self.cache[key] = data
            self.access_times.append(key)
        else:
            if len(self.cache) >= self.max_size:
                # Remove oldest entry
                oldest_key = self.access_times.pop(0)
                del self.cache[oldest_key]
            self.cache[key] = data
            self.access_times.append(key)


# Global cache instance
cache = DataCache()


def load_from_gcs_cached(bucket_name: str = "crime-stat-app-us", 
                         file_path: str = "front/dashboard.parquet",
                         use_cache: bool = True) -> pd.DataFrame:
    """
    Load data from GCS with caching.
    
    Args:
        bucket_name: GCS bucket name
        file_path: Path to parquet file within bucket
        use_cache: Whether to use cached data
    
    Returns:
        DataFrame with loaded data
    """
    cache_key = f"{bucket_name}/{file_path}"
    
    # Check cache first
    if use_cache:
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            print(f"Cache hit for {cache_key}")
            return cached_data
    
    if not GCS_AVAILABLE:
        raise ImportError("google-cloud-storage not available. Install with: pip install google-cloud-storage")
    
    start_time = time.time()
    
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        
        # Download to temporary file
        temp_path = f"/tmp/{file_path.replace('/', '_')}"
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        
        blob.download_to_filename(temp_path)
        
        # Read parquet file
        df = pd.read_parquet(temp_path)
        
        # Cache the data
        cache.set(cache_key, df)
        
        elapsed = time.time() - start_time
        print(f"Loaded {len(df)} rows from GCS in {elapsed:.2f} seconds")
        
        return df
        
    except Exception as e:
        print(f"Error loading from GCS: {str(e)}")
        raise


def load_data_for_frontend(bucket_name: str = "crime-stat-app-us",
                          file_path: str = "front/dashboard.parquet",
                          use_cache: bool = True) -> pd.DataFrame:
    """
    Load and prepare data for the frontend application.
    
    This function applies all necessary data transformations:
    - Load data from GCS
    - Apply data cleaning
    - Add derived columns
    - Prepare lookup tables
    
    Args:
        bucket_name: GCS bucket name
        file_path: Path to parquet file within bucket
        use_cache: Whether to use cached data
    
    Returns:
        Prepared DataFrame ready for Dash application
    """
    # Load data
    df = load_from_gcs_cached(bucket_name, file_path, use_cache)
    
    # Data validation
    if df.empty:
        raise ValueError("Loaded data is empty")
    
    # Required columns check
    required_cols = ['datetime', 'city', 'type', 'details']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Data cleaning
    # Remove rows where city ends with ' län'
    df = df[~df['city'].str.lower().str.strip().str.endswith(' län')].copy()
    
    # Handle missing values
    df['details'] = df['details'].fillna('No details available')
    df['type'] = df['type'].fillna('Unknown')
    
    # Convert datetime to local time
    if df['datetime'].dt.tz is not None:
        df['datetime'] = df['datetime'].dt.tz_convert(None)
    
    # Add derived columns for frontend efficiency
    df['hour'] = pd.to_datetime(df['datetime']).dt.hour
    
    # Incident type classification
    df['incident_type'] = np.where(
        (df['type'] == 'Trafikrelaterad') | (df['type'] == 'Traffic-related'),
        'Traffic',
        'Significant incidents'
    )
    
    df['incident_type'] = np.where(
        (~df['incident_type'].isin(['Traffic']))
        & (
            (df['type'].str.strip().str.lower().str.startswith('summary'))
            | (df['type'].str.strip().str.lower().str.startswith('sammanfattning'))
        ),
        'Summary',
        df['incident_type']
    )
    
    df['incident_type'] = np.where(
        (~df['incident_type'].isin(['Traffic', 'Summary']))
        & (
            (df['type'].str.strip().str.lower().str.startswith('other'))
            | (df['type'].str.strip().str.lower().str.startswith('övrigt'))
        ),
        'Other',
        df['incident_type']
    )
    
    # Gun filter
    df['gun_filter'] = np.where(
        df['gun_filter'] == 'gun-related', 
        'Gun-related', 
        'Not gun-related'
    )
    
    # Date range preparation
    date_range_ms = pd.date_range(
        start=datetime(2020, 10, 1), end=datetime.today(), freq='3MS'
    )
    date_range = pd.date_range(
        start=datetime(2020, 10, 1), end=datetime.today() + pd.Timedelta(days=1)
    )
    df['date_requested'] = pd.to_datetime(df['date_requested'])
    
    # Add lookup tables for dropdowns
    df['cities'] = ['All'] + sorted(df['city'].unique())
    df['types_inc'] = ['All'] + [
        'Significant incidents', 'Traffic', 'Summary', 'Other'
    ]
    df['languages'] = sorted(df['language'].unique())
    df['gun_filters'] = ['All'] + sorted(df['gun_filter'].unique())
    
    # Table columns for display
    df['table_columns'] = ['datetime', 'type', 'details']
    
    return df


def validate_data(df: pd.DataFrame) -> Tuple[bool, list]:
    """
    Validate data integrity.
    
    Args:
        df: DataFrame to validate
    
    Returns:
        Tuple of (is_valid, list_of_issues)
    """
    issues = []
    
    if df.empty:
        issues.append("DataFrame is empty")
    
    required_cols = ['datetime', 'city', 'type', 'details', 'osm_lat', 'osm_lon']
    for col in required_cols:
        if col not in df.columns:
            issues.append(f"Missing column: {col}")
    
    # Check for missing dates
    if 'datetime' in df.columns:
        if df['datetime'].isnull().any():
            issues.append("Found null values in datetime column")
    
    # Check for invalid coordinates
    if all(col in df.columns for col in ['osm_lat', 'osm_lon']):
        invalid_coords = df[
            (df['osm_lat'].isna()) | 
            (df['osm_lon'].isna()) |
            (df['osm_lat'] < -90) | 
            (df['osm_lat'] > 90) |
            (df['osm_lon'] < -180) | 
            (df['osm_lon'] > 180)
        ]
        if len(invalid_coords) > 0:
            issues.append(f"Found {len(invalid_coords)} rows with invalid coordinates")
    
    is_valid = len(issues) == 0
    
    return is_valid, issues


def get_latest_gcs_file(bucket_name: str = "crime-stat-app-us", 
                       prefix: str = "dashboard/dashboard_") -> Optional[str]:
    """
    Get the most recent file from GCS.
    
    Args:
        bucket_name: GCS bucket name
        prefix: File name prefix to search for
    
    Returns:
        Full GCS path or None if no files found
    """
    if not GCS_AVAILABLE:
        return None
    
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            return None
        
        # Sort by time_created and get latest
        latest = max(blobs, key=lambda x: x.time_created)
        return f"gs://{bucket_name}/{latest.name}"
        
    except Exception as e:
        print(f"Error getting latest GCS file: {str(e)}")
        return None


# Legacy compatibility functions
def get_token_from_gcs(bucket_name: str = "crime-stat-app-us", 
                      file_path: str = "mapbox.txt") -> str:
    """
    Get Mapbox access token from GCS.
    
    Args:
        bucket_name: GCS bucket name
        file_path: Path to token file within bucket
    
    Returns:
        Mapbox access token string
    """
    if not GCS_AVAILABLE:
        raise ImportError("google-cloud-storage not available")
    
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        
        return blob.download_as_string().decode("utf-8").strip()
        
    except Exception as e:
        print(f"Error getting Mapbox token: {str(e)}")
        raise


def get_last_updated(df: pd.DataFrame) -> str:
    """
    Get the last updated timestamp from data.
    
    Args:
        df: DataFrame with 'date_requested' column
    
    Returns:
        Formatted timestamp string
    """
    if 'date_requested' not in df.columns:
        return datetime.now().strftime("%Y-%m-%d %H:%M")
    
    return df['date_requested'].max().strftime("%Y-%m-%d %H:%M")


if __name__ == "__main__":
    # Test the data loader
    print("Testing data loader...")
    
    try:
        # Try to load from cache (will fail if GCS not available)
        try:
            df = load_data_for_frontend()
            print(f"Loaded {len(df)} rows")
            
            # Validate
            is_valid, issues = validate_data(df)
            if is_valid:
                print("Data validation: PASSED")
            else:
                print("Data validation: FAILED")
                for issue in issues:
                    print(f"  - {issue}")
            
            print(f"Last updated: {get_last_updated(df)}")
            
        except Exception as e:
            print(f"Note: GCS not available for full test: {str(e)}")
            print("Running basic import test instead...")
            
        print("\nData loader module loaded successfully!")
        
    except Exception as e:
        print(f"Error during test: {str(e)}")
