# Spotify Top Charts ETL Pipeline  
## Overview  
This project is an automated ETL (Extract, Transform, Load) pipeline that retrieves daily top charts data from the Spotify API, processes it, and loads it into Snowflake for analysis.  
## Tech Stack  
- **AWS Lambda** – Extracts Spotify top charts data from the Spotify API  
- **Amazon S3** – Stores raw JSON data  
- **Python (Pandas) in AWS Lambda** – Transforms raw data into structured CSV files  
- **AWS CloudWatch** – Triggers transformation upon new data arrival  
- **Snowflake & SnowPipe** – Loads and stores transformed data for analysis  
- **Snowflake Analytics** – Visualizes streaming trends  
## Workflow  
1. **Extraction:** An AWS Lambda function fetches daily top charts from the Spotify API and stores the raw JSON data in an S3 bucket.  
2. **Transformation:**  
  - A second AWS Lambda function, triggered by AWS CloudWatch, processes the raw JSON data.  
  - The function converts the raw JSON into structured CSV files and saves them in another S3 bucket.  
3. **Loading into Snowflake:**  
  - Whenever new transformed data is added to the S3 bucket, Snowflake is automatically notified.  
  - SnowPipe is triggered and ingests the latest data into Snowflake without manual intervention.  
  - The data is then available for querying and analysis using Snowflake Analytics.  
4. **Visualization:** Snowflake Analytics is used to generate insights and visualize streaming trends.  
