# Performance Improvement Plan for CrimeStats Web App

## Executive Summary

The crime-stats-production web application has performance issues that make it slow and unresponsive. This document provides a comprehensive plan to address these issues, with particular attention to fixing the broken data pipeline that has caused missing data for several years.

---

## Current State Analysis

### Key Issues Identified

1. **Broken Data Pipeline**
   - Data processing pipeline fails partway through
   - Raw data is not saved before processing
   - Missing data for 2+ years in BigQuery
   - No fallback mechanisms for processing failures

2. **Slow Frontend Interactivity**
   - Data loaded on every server start
   - No caching of processed data
   - Large dataframes processed in every callback
   - No pagination for data tables
   - Map visualizations without clustering

3. **Architecture Problems**
   - Tight coupling between data processing and frontend
   - No separation of concerns
   - Missing error handling
   - No monitoring or logging

---

## Recommended Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA PROCESSING LAYER                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐             │
│  │  RAW DATA    │ →  │   CLEAN &    │ →  │  PROCESSED   │             │
│  │   (API)      │    │   TRANSFORM  │    │   DATA       │             │
│  └──────────────┘    └──────────────┘    └──────────────┘             │
│         │                      │                   │                   │
│         ▼                      ▼                   ▼                   │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐             │
│  │   GCS RAW    │    │   GCS RAW    │    │  GCS FINAL   │             │
│  │  STORAGE     │    │   TRANSFORM  │    │   STORAGE    │             │
│  └──────────────┘    └──────────────┘    └──────────────┘             │
│         │                              │                   │             │
│         └──────────┬───────────────────┘                   │             │
│                    │                                       │             │
│                    ▼                                       ▼             │
│            ┌──────────────┐                      ┌──────────────┐       │
│            │  BIGQUERY    │                      │   GCS FINAL    │       │
│            │   (Primary)  │                      │  (Frontend)    │       │
│            └──────────────┘                      └──────────────┘       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        FRONTEND LAYER                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐             │
│  │  DASH APP    │ ←  │   GCS CACHE  │ ←  │   FAST API   │             │
│  │  (Frontend)  │    │   LAYER      │    │  (Optional)  │             │
│  └──────────────┘    └──────────────┘    └──────────────┘             │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Fix Broken Data Pipeline (Immediate - 2-3 days)

### Problem
The data pipeline breaks partway through processing, causing missing data for 2+ years. The pipeline doesn't save raw data before processing, making recovery impossible.

### Solution: GCS-First Approach

#### 1.1 Save Raw Data to GCS First

**Current Issue:**
```python
# Current (BROKEN) approach
new_data = request_api()  # Fetch from API
# Process data immediately
update_table_raw()  # If this fails, data is lost
```

**Fixed Approach:**
```python
# NEW (FIXED) approach
new_data = request_api()  # Fetch from API

# STEP 1: Save raw data to GCS FIRST (critical!)
save_to_gcs_raw(new_data, project_id, dataset_id, table_id)

# STEP 2: Only process if raw data is safely stored
update_table_raw()  # Now we can recover if this fails
```

#### 1.2 Data Pipeline Steps

The improved pipeline will:

1. **Fetch raw data from API**
2. **Save to GCS as parquet** (immediate, guaranteed storage)
3. **Log the GCS path** for tracking
4. **Process the data** from GCS (now recoverable)
5. **Save processed data to BigQuery**
6. **Save final processed data to GCS** for frontend use

#### 1.3 Error Handling

Add comprehensive error handling with retry logic:

```python
def save_to_gcs_raw_with_retry(df, project_id, dataset_id, table_id, max_retries=3):
    for attempt in range(max_retries):
        try:
            gcs_path = save_to_gcs_raw(df, project_id, dataset_id, table_id)
            return gcs_path
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            sleep(2 ** attempt)  # Exponential backoff
```

#### 1.4 Data Recovery

The GCS-first approach enables data recovery:

```python
def recover_raw_data_from_gcs(project_id, dataset_id, table_id):
    """Load the last saved raw data from GCS for recovery."""
    gcs_path = get_latest_gcs_raw_file(project_id, dataset_id, table_id)
    if gcs_path:
        return pd.read_parquet(gcs_path)
    return None
```

---

## Phase 2: Frontend Performance Optimization (Short-term - 1 week)

### 2.1 Data Loading Optimization

**Problem:** Data is loaded from GCS on every server start and every callback.

**Solutions:**

#### a) Server-Side Caching

Implement Redis caching for processed data:

```python
from redis import Redis
import pickle

redis_client = Redis(host='localhost', port=6379, decode_responses=True)

def get_processed_data():
    cached = redis_client.get('processed_data')
    if cached:
        return pickle.loads(cached)
    
    # Load from GCS
    df = load_from_gcs_cached()
    
    # Cache for future requests
    redis_client.set('processed_data', pickle.dumps(df), ex=3600)  # 1 hour TTL
    return df
```

#### b) Incremental Data Loading

Only load changed data:

```python
def load_incremental_data(project_id, dataset_id, last_loaded_timestamp):
    """Load only data changed since last load."""
    QUERY = f"""
        SELECT * FROM `{project_id}.{dataset_id}.dashboard`
        WHERE date_requested > TIMESTAMP("{last_loaded_timestamp}")
    """
    return pandas_gbq.read_gbq(QUERY, project_id=project_id)
```

#### c) Parquet Partitioning

Partition data by date for faster queries:

```python
df.to_parquet(
    'gs://bucket/processed_data/',
    partition_cols=['year', 'month'],  # Partition by time
    compression='snappy'  # Efficient compression
)
```

### 2.2 Callback Optimization

**Problem:** Multiple callbacks repeat the same data filtering logic.

**Solution:** Create a unified data filtering function:

```python
def filter_data_cached(df, date_range, city, type_inc, gun, language):
    """Cached data filtering that can be reused across callbacks."""
    key = f"filter_{date_range}_{city}_{type_inc}_{gun}_{language}"
    
    # Check cache
    cached = cache.get(key)
    if cached is not None:
        return cached
    
    # Apply filters
    min_date, max_date = date_range
    cities_loc = [city] if city != "All" else cities
    type_inc_list = [type_inc] if type_inc != "All" else types_inc
    gun_list = [gun] if gun != "All" else gun_filters
    
    dff = df[
        (df["datetime"] <= max_date) &
        (df["datetime"] >= min_date) &
        (df["city"].isin(cities_loc)) &
        (df["language"] == language) &
        (df["incident_type"].isin(type_inc_list)) &
        (df["gun_filter"].isin(gun_list))
    ].copy()
    
    # Cache the result
    cache.set(key, dff)
    return dff
```

### 2.3 Map Visualization Optimization

**Problem:** Large scatter mapbox plots are slow to render.

**Solutions:**

#### a) Map Clustering

Enable clustering for better performance:

```python
fig = px.scatter_mapbox(
    map_agg,
    lat="lat",
    lon="lon",
    cluster_id="cluster_id",  # Enable clustering
    # ... other parameters
)
```

#### b) Heatmaps for Large Datasets

Use heatmaps instead of individual markers for large datasets:

```python
fig = px.density_mapbox(
    dff,
    lat='osm_lat',
    lon='osm_lon',
    radius=10,
    zoom=zoom,
    # ... other parameters
)
```

#### c) Reduce Data Points

Sample data for visualization:

```python
# For map visualization, sample to 1000 points max
map_agg = map_agg.sample(min(1000, len(map_agg)), random_state=42)
```

### 2.4 Table Performance

**Problem:** Full table loaded at once, no pagination.

**Solutions:**

#### a) Server-Side Pagination

```python
@app.callback(
    Output("main_table", "data"),
    [Input("main_table", "page_current"), 
     Input("main_table", "page_size")]
)
def update_table_page(page_current, page_size):
    start = page_current * page_size
    end = start + page_size
    return df[table_columns].iloc[start:end].to_dict('records')
```

#### b) Virtual Scrolling

Enable virtual scrolling for large tables:

```python
dash_table.DataTable(
    id="main_table",
    virtualization=True,  # Enable virtual scrolling
    page_size=50,  # Limit to 50 rows initially
    # ... other parameters
)
```

#### c) Lazy Loading

Load data on demand:

```python
# Only load table data when table is actually viewed
@app.callback(
    Output("table_placeholder", "children"),
    [Input("show_table_button", "n_clicks")]
)
def load_table_data(n_clicks):
    if n_clicks is None:
        return "Click to load table data"
    
    # Load and display table
    return dash_table.DataTable(...)
```

### 2.5 Chart Optimization

**Problem:** Charts are regenerated from scratch on every filter change.

**Solutions:**

#### a) Chart State Preservation

Preserve chart state between interactions:

```python
@app.callback(
    Output("chart_state", "data"),
    [Input("type-chart", "relayoutData")],
    [State("chart_state", "data")]
)
def save_chart_state(relayoutData, chart_state):
    if relayoutData:
        chart_state = relayoutData
    return chart_state
```

#### b) Client-Side Calculations

Move simple calculations to client-side:

```python
app.clientside_callback(
    """
    function(value) {
        return value * 2;
    }
    """,
    Output("output", "children"),
    Input("input", "value")
)
```

---

## Phase 3: Infrastructure Improvements (Medium-term - 2-3 weeks)

### 3.1 Database Optimization

#### a) BigQuery Partitioning

Partition BigQuery tables by date:

```sql
CREATE TABLE `project.dataset.raw`
PARTITION BY DATE(datetime)
AS SELECT * FROM `project.dataset.raw_temp`;
```

#### b) BigQuery Clustering

Cluster tables by frequently filtered columns:

```sql
CREATE TABLE `project.dataset.raw`
PARTITION BY DATE(datetime)
CLUSTER BY city, incident_type;
```

#### c) Materialized Views

Create materialized views for common queries:

```sql
CREATE MATERIALIZED VIEW `project.dataset.daily_summary`
AS SELECT
    DATE(datetime) as date,
    city,
    COUNT(*) as incident_count
FROM `project.dataset.raw`
GROUP BY DATE(datetime), city;
```

### 3.2 Caching Infrastructure

#### a) Redis Cache

Deploy Redis for server-side caching:

```python
# Redis configuration
redis_client = Redis(
    host=os.environ.get('REDIS_HOST', 'localhost'),
    port=int(os.environ.get('REDIS_PORT', 6379)),
    decode_responses=True
)

# Cache settings
CACHE_TTL = 3600  # 1 hour
CACHE_PREFIX = "crime_stats"
```

#### b) Mapbox Tile Caching

Cache Mapbox tiles at the CDN level.

### 3.3 Horizontal Scaling

#### a) Cloud Run Scaling

Configure Cloud Run for horizontal scaling:

```yaml
# cloudbuild.yaml
- name: 'gcr.io/cloud-builders/gcloud'
  args:
  - 'run', 'deploy', 'crime-stats-frontend'
  - '--platform', 'managed'
  - '--min-instances', '2'  # Minimum instances
  - '--max-instances', '10'  # Maximum instances
```

#### b) Load Balancing

Use Cloud Load Balancing for traffic distribution.

---

## Phase 4: Monitoring and Alerting (Ongoing)

### 4.1 Logging

Add comprehensive logging:

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"Pipeline started for project {project_id}")
logger.info(f"Fetched {len(df)} records from API")
logger.warning(f"Scraping failed for URL: {url}")
logger.error(f"Pipeline failed: {str(e)}")
```

### 4.2 Monitoring

Set up Cloud Monitoring alerts:

- Data pipeline failures
- Data freshness thresholds
- Frontend latency > 2 seconds
- Error rate > 1%

### 4.3 Health Checks

Add health check endpoints:

```python
@app.route("/health")
def health_check():
    try:
        # Check GCS connection
        client = storage.Client()
        client.bucket("crime-stat-app-us").exists()
        
        # Check BigQuery connection
        bq_client = bigquery.Client()
        bq_client.query("SELECT 1").result()
        
        return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}, 500
```

---

## Implementation Priority

### Priority 1: Critical (1 week)
1. ✅ Implement GCS-first data pipeline
2. ✅ Add raw data saving before processing
3. ✅ Add error handling and retry logic
4. ✅ Fix data recovery mechanism

### Priority 2: Important (2 weeks)
5. Implement Redis caching layer
6. Add data partitioning in BigQuery
7. Enable map clustering
8. Add server-side table pagination
9. Optimize callback logic

### Priority 3: Nice to Have (3-4 weeks)
10. Implement FastAPI for data serving
11. Add horizontal scaling
12. Set up monitoring and alerting
13. Create data quality dashboard

---

## Migration Strategy

### Step 1: Backup Current Data
```bash
# Backup existing BigQuery data
bq extract \
  --format=PARQUET \
  project:dataset.raw \
  gs://crime-stat-app-us/backup/raw_backup.parquet
```

### Step 2: Deploy Improved Pipeline
1. Deploy improved data pipeline with GCS-first approach
2. Run initial data fetch for past 2 years
3. Verify data integrity
4. Update frontend to use new data format

### Step 3: Monitor and Iterate
1. Monitor pipeline for 1 week
2. Fix any issues that arise
3. Gradually optimize frontend
4. Measure performance improvements

---

## Performance Metrics to Track

### Data Pipeline
- API fetch time: Target < 5 seconds
- Raw data saving time: Target < 3 seconds
- Data processing time: Target < 30 seconds
- BigQuery load time: Target < 10 seconds

### Frontend
- Page load time: Target < 3 seconds
- Filter response time: Target < 1 second
- Map render time: Target < 2 seconds
- Table pagination: Target < 0.5 seconds

### Infrastructure
- GCS storage cost: Monitor and optimize
- BigQuery query cost: Monitor and optimize
- Cloud Run instances: Scale based on usage

---

## Cost Optimization

### GCS Storage
- Use Nearline/Archive for old data
- Implement lifecycle policies

### BigQuery
- Use partitioned tables
- Use clustering for filtered queries
- Limit query scope with date filters

### Cloud Run
- Scale to zero when not in use
- Use minimum instances during peak hours

---

## Conclusion

This improvement plan addresses the broken data pipeline and slow frontend performance through:

1. **GCS-First Approach**: Ensures data durability and enables recovery
2. **Caching Layer**: Reduces redundant processing
3. **Optimized Visualizations**: Improves interactivity
4. **Infrastructure Scaling**: Ensures reliability

The phased approach allows for incremental improvements while maintaining system stability.

---

## Next Steps

1. ✅ Review and approve this improvement plan
2. ✅ Set up GCS bucket for data backup
3. ✅ Deploy improved data pipeline
4. ✅ Monitor and validate data quality
5. ✅ Implement frontend optimizations
6. ✅ Set up monitoring and alerting

---

**Document Version**: 1.0  
**Last Updated**: 2025-01-22  
**Author**: AI Performance Analyst  
**Status**: Ready for Implementation
