# API DATA UPLOADER 

Solution for generating automated data flows: run scheduled procedures via Airflow.

Each script performs the following procedures:
1) get data via API 
2) ETL processing
3) download Data to Postgres Database

The Data is accumulated in Database, then used in BI tools (Tableau, Power BI) to build Business intelligence dashboards.

## In [reposytory](https://github.com/AnastasiiaFomich/API-dataflows), 
There are several templates:
1) [Facebook API](https://github.com/AnastasiiaFomich/API-dataflows/blob/main/Airflow%20DAG:%20get%20FB%20API%20data%20with%20Python.py) application. Gains Data of FB Ads Stats (clicks, impressions, spends)
2) [Yandex Direct API](https://github.com/AnastasiiaFomich/API-dataflows/blob/main/Airflow%20DAG:%20get%20Yandex%20Direct%20API%20data%20with%20Python.py)
3) [MyTarget API](https://github.com/AnastasiiaFomich/API-dataflows/blob/main/Airflow%20DAG:%20get%20MyTarget%20API%20data%20with%20Python.py)

Each application is developed considerinng API's documentations and requirements, including time and data volume limits.





