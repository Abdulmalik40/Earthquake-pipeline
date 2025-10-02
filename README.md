Earthquake Data Pipeline
This repository contains a Databricks-based data pipeline that fetches earthquake data from the USGS FDSN Event Web Service, processes it through bronze → silver → gold layers, and enriches it with country codes using reverse geocoding.

 Pipeline Overview
Bronze Layer
Fetches raw earthquake data (GeoJSON) from USGS API for a given date range.
Stores raw JSON in ADLS (Azure Data Lake Storage).
Silver Layer
Parses and flattens the JSON.
Cleans and converts Unix timestamps to proper TimestampType.
Saves as Parquet in ADLS.
Gold Layer
Enriches data with country codes using reverse_geocoder.
Adds a significance class (Low / Moderate / High) based on USGS sig value.
Outputs final curated dataset as Parquet.
🛠️ Requirements
Databricks workspace (with access to ADLS)
Python libraries: requests, reverse_geocoder
Azure storage account with containers: bronze, silver, gold
Valid date range (USGS API supports historical and real-time data)
🔧 Configuration
Update the following in your notebook/job parameters:

start_date and end_date (format: YYYY-MM-DD)
ADLS account name (lukedbx in example — replace with yours)
 USGS API Used
Endpoint: https://earthquake.usgs.gov/fdsnws/event/1/query
Format: geojson
Documentation: FDSN Event Web Service
