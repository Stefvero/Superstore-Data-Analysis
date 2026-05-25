# Superstore Sales Dataset analysis

## Repository Outline
```
1. P2M3_Stefano_Veronigo_Wijaya_ddl.txt - DDL SQL query
2. P2M3_Stefano_Veronigo_Wijaya_data_raw.csv - Raw data from PostGreSQL
3. P2M3_Stefano_Veronigo_Wijaya_data_clean.csv - Cleaned data from airflow
4. P2M3_Stefano_Veronigo_Wijaya_DAG.py - Airflow DAG file
5. P2M3_Stefano_Veronigo_Wijaya_DAG_graph.jpg - Screenshot of DAG graph in Airflow
6. P2M3_Stefano_Veronigo_Wijaya_GX.ipynb - Great Expectations notebook
7. images (folder) - Kibana visualizations and insights
```

## Problem Background
`This project is made to analyze sales performance and customer behavior within a superstore using the Superstore dataset. In a competitive retail environment, companies need to understand which products generate the highest revenue, which regions contribute the most profit, and how customer segments behave.`

## Project Output
Project output consists of:
- Automated data cleaning pipeline using Airflow
- Cleaned dataset for further analysis
- Validated dataset using Great Expectations
- Stored processed data with ElasticSearch 
- Interactive Visualization in Kibana

## Data
The dataset used in this project is the Superstore Dataset obtained from Kaggle: <br>
https://www.kaggle.com/datasets/vivek468/superstore-dataset-final <br>
Data has 9994 rows and 21 columns <br>
Clean dataset has their columns standardized and missing values handles with median for numerical and mode for categorical columns.

## Method
1. Store raw .csv file using PostGreSQL that was ran using Docker
2. Using Airflow, fetch data from PostGreSQL, clean the data, store cleaned data as a .csv file
3. Load data into ElasticSearch
4. Validate Data using Great Expectation
5. Visualize data with Kibana
## Stacks
- Language: Python
- Tools:
    - Apache Airflow
    - PostGreSQL (from Docker)
    - ElasticSearch
    - Kibana
- Libraries:
    - pandas
    - psycopg2
    - great_expectations
    - elasticsearch
    - datetime

## Reference
- Apache Documentation              : https://airflow.apache.org/docs/ + class recording
- ElasticSearch Documentation       : class recording
- Kibana Documentation              : class recording
- Great Expectations Documentation  : https://greatexpectations.io/expectations/ + class recording

---

**Additional References:**
- [GX tutorial](https://www.datacamp.com/tutorial/great-expectations-tutorial)