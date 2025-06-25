# NYC Yellow-Taxi Duration Prediction with Spark

[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/spark-3.5.0-orange)](https://spark.apache.org/)
[![Dataset Size](https://img.shields.io/badge/dataset-480%20MB-green)](#dataset)
[![Last Commit](https://img.shields.io/github/last-commit/turancannb02/bigdata-taxi-spark)](https://github.com/turancannb02/bigdata-taxi-spark/commits/main)

A single-container Apache Spark environment and executed Jupyter notebook that demonstrate big-data ETL and a Random-Forest regression model for predicting yellow-taxi trip duration.  
Created for the Big-Data Analytics assignment (Semester 3, BSBI).

---

## 1. Business context

* **Problem**  
  Dispatch must provide accurate estimated time of arrival (ETA) at the moment a passenger is picked up.

* **Big-data value**  
  May 2025 alone logs 4.6 million trips; historical data covers years. Modelling on this volume improves ETA accuracy, reduces rider cancellations and helps the city tune congestion policy.

* **Challenges**  
  Volume 4.6 million rows per month; velocity near-real-time feeds; variety timestamps, categorical IDs and numerics; privacy rules for geolocation.

---

## 2. Environment

| Layer          | Choice                               |
| -------------- | ------------------------------------ |
| Compute        | Apache Spark 3.5.0 local[*]          |
| Storage        | Parquet on host bind-mount           |
| MLlib          | RandomForestRegressor (50 trees)     |
| Orchestration  | Docker Compose (single service)      |
| Notebook       | JupyterLab 4 in a Python virtualenv  |


<pre><code>
bigdata-lab/
  ├── data/ # Parquet files 
  ├── venv/ # Python virtual environment 
  ├── docker-compose.yml # Single-service Spark container 
  ├── nyc_taxi_duration.ipynb # Executed notebook with model results 
  ├── README.md # Project description and usage 
  ├── requirements.txt # Python dependencies 
  └── test.py # test script to check the .parquet file
</code></pre>

### Quick start

```bash
git clone https://github.com/turancannb02/bigdata-taxi-spark.git
cd bigdata-taxi-spark
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
jupyter lab
```

## 3. Dataset 

- File yellow_tripdata_2025-05.parquet
- Size ≈ 480 MB
- Rows 4 591 845
- Source [TLC Trip-Records CDN](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
  - [May 2025 - Yellow Taxi Trip Records](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-05.parquet)

Download manually or run: 
  ```bash
  mkdir -p data
  curl -L -o data/yellow_tripdata_2025-05.parquet \
      https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-05.parquet
  ```

## 4. Spark ETL
---
```bash
df.shape   → (4 591 845, 20)
duration_min  = (dropoff_ts − pickup_ts) / 60
filter        0 < duration < 180
add           hour, wday
clean/drop    null rows
sample        50 %   → 2 296 080 rows
```

## 5. Machine learning

| Setting        | Value                      |
|----------------|----------------------------|
| **Algorithm**  | Random-Forest Regression   |
| **Trees**      | 50                         |
| **Max depth**  | 12                         |
| **Sample size**| 2.3 million rows (50 %)    |
| **Driver heap**| 4 GB                       |

**Metrics on 50 % sample**

| RMSE (min) | MAE (min) | R²  |
|------------|-----------|-----|
| **6.72**   | **4.12**  | **0.81** |

Top three feature importances: `trip_distance`, `hour`, `PULocationID`.


