#from sqlalchemy import create_engine from datetime import datetime from airflow import DAG from airflow.providers.postgres.operators.postgres import PostgresOperator from airflow.providers.postgres.hooks.postgres import PostgresHook import 
#requests, json from requests.exceptions import ConnectionError from time import sleep import pandas as pd from airflow.operators.python_operator import PythonOperator

from sqlalchemy import create_engine
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta, date, datetime
import requests, json
from requests.exceptions import ConnectionError
from time import sleep
import pandas as pd
from airflow.operators.python_operator import PythonOperator

dag_params = {
    'dag_id': 'PostgresOperator_dag_ya_new',
    'start_date': datetime(2020, 9, 28, hour=11, minute=00, second=0),
    'schedule_interval':  '10-59/30 * * * *'
}

import sys

if sys.version_info < (3,):
    def u(x):
        try:
            return x.encode("utf8")
        except UnicodeDecodeError:
            return x
else:
    def u(x):
        if type(x) == type(b''):
            return x.decode('utf8')
        else:
            return x
        
#declare private credentials:        
token = ''
clientLogin = ''

sandbox_url = 'https://api.direct.yandex.com/json/v5/agencyclients'

headers = {"Authorization": "Bearer " + token,  # OAuth-token
           "Accept-Language": "ru", 
           }

body = {"method": "get",  
        "params": {'FieldNames':  ["AccountQuality" , "Archived", "ClientInfo", "Login" ]                   
                  ,'SelectionCriteria' :{}
                  }}

jsonBody = json.dumps(body, ensure_ascii=False).encode('utf8')

body = json.dumps(body, indent=4)
req = requests.post(sandbox_url, jsonBody, headers=headers)
flat2 = req.text

#declare Python function for DAG Task 1 (get account_IDs list):
def get_clients_df(**context):
    global flat2 
    clients_df = pd.read_json(flat2)
    #check correctness of DF:
    if clients_df.columns[0] == 'result' and clients_df.index.tolist()[0] == 'Clients':
        #print('yes')
        flat3  = flat2[21:-2]
        #print(flat2)
        clients_df = pd.read_json(flat3)
        if list(clients_df.columns) == ['AccountQuality', 'Archived', 'Login', 'ClientInfo']:
            pass
        else:
            raise KeyboardInterrupt
        clients = set(clients_df['Login'])
        context['ti'].xcom_push(key = 'clients', value = clients)
        return clients
    
#declare Python function for DAG Task 2 (get full data statystics):
def get_df( **context):
    clients = context['ti'].xcom_pull(key = 'clients')
    global pd
    # def function to get statistics DataFrame:
    def func2(clients):
        all_ = pd.DataFrame()
        #generate list of Dates:
        list_of_dates_corr = []
        for stack in range(30, 35):
            day = str(date.today()-timedelta(days=stack))
            list_of_dates_corr.append(day)
        def func(client, now_,date_):
            i = 0
            print(now_)
            global pd
            print(client, now_)
            all_2 = pd.DataFrame()
            import time
            token = ''
            client_login = client
            sandbox_url = 'https://api.direct.yandex.com/json/v5/reports/'
            headers = {
                'Authorization' : 'Bearer ' + token,
                'Client-Login': client_login,
                'returnMoneyInMicros':'false',
                "Accept-Language": "ru",
                'processingMode': 'offline'
            }
            body = {
                "params": {
                    "SelectionCriteria": {
                        "DateFrom": date_,
                        "DateTo": date_
                    },
                    "FieldNames": [
                        'Date',
                        'AdGroupId','AdGroupName','AdId',
                        'CampaignId',
                        'CampaignName',
                        'Impressions',
                        'Clicks', 
                        'Bounces', 'Cost', 'Sessions'

                    ],
                    "ReportName": "Мой тестовый отчет из песочницы_{}".format(now_),
                    "ReportType": "AD_PERFORMANCE_REPORT",
                    "DateRangeType": "CUSTOM_DATE",
                    "Format": "TSV",
                    "IncludeVAT": "NO",
                    "IncludeDiscount": "NO",

                }
            }

            body = json.dumps(body, indent=4)
            req = requests.post(sandbox_url, body, headers=headers)
            if req.status_code == 200:

                flat = req.text
                print('FLAT' ,client,  len(flat), flat)

                import sys
                if sys.version_info[0] < 3: 
                    from StringIO import StringIO
                else:
                    from io import StringIO

                import pandas as pd
                TESTDATA = StringIO(flat)
                df = pd.read_csv(TESTDATA, sep="\t",header = [1], error_bad_lines=False)

                df['client'] = client

                all_2 = all_2.append(df)
                print('hello_',all_2)

            elif (req.status_code == 201) or (req.status_code == 202):
                time.sleep(int(req.headers['retryIn']))

                all_2 = func(client, now_, date_)

            return all_2
        for date_ in list_of_dates_corr:
            for client in clients:
                now_ = str(datetime.now())

                df = func(client, now_, date_)
                print('hi_',df)
                all_ = all_.append(df)
        return all_

    all_ = func2(clients)
    all_['Business_date'] = datetime.now().replace(microsecond=0)
    all_['Business_date'] =  pd.to_datetime(all_['Business_date'], errors='coerce')
    all_.rename(columns={
    'Date':'Date',
    'account_name':'Client_name',
    'AdGroupName':'AdGroup_Name',
    'AdGroupId':'AdGroup_Id',
    'AdId':'Ad_id',
    'CampaignName':'Campaign_name',
    'CampaignId':'Campaign_id',
    'spend':'Spend',
    'client':'Client_name',
    'Cost':'Spend'},inplace=True)

    all_['Date'] =  pd.to_datetime(all_['Date'], errors='coerce').dt.date
    all_['Platform']='YA'
    all_ = all_[all_['AdGroup_Id'].isnull() == False]
    all_ = all_.drop_duplicates()
    all_['Ad_id'] = all_['Ad_id'].astype(int)
    all_['Campaign_id'] = all_['Campaign_id'].astype(int)
    all_['Ad_id'][all_['Ad_id'].isnull()==False] = all_['Ad_id'].astype(int)
    all_['Campaign_id'][all_['Campaign_id'].isnull()==False] = all_['Campaign_id'].astype(int)

    print(all_.shape)
    context['ti'].xcom_push(key = 'clients_df', value = all_) 
    all_= all_.drop_duplicates()
    return all_
    
engine ='postgresql+psycopg2://airflow@localhost:8123/digitaladsdb'
def upload_data_to_db(**context):
#    postgres_hook = PostgresHook(self.postgres_conn_id)
    global engine
#engine = postgres_hook.get_sqlalchemy_engine()
    table = context['ti'].xcom_pull(key = 'clients_df') 
   # engine = create_engine('postgresql://airflow:y59RaFJKqy@192.168.127.16:8123/digitalads')
    table.to_sql('yandex_pre2', engine, if_exists='append', index = False)

with DAG(**dag_params) as dag:

    create_table = PythonOperator(
        task_id='download_data',
        provide_context = True,
        python_callable=get_clients_df,
        dag=dag,
    )
#    print(type(create_table))    
#with DAG(**dag_params) as dag:
    create_table2 = PythonOperator(
        task_id='download_data2',
        python_callable=get_df,
        dag=dag,
        provide_context = True,
    )
#    insert_row = PostgresOperator(
 #   task_id='insert_row',
  #  sql="INSERT INTO yandex1 VALUES (timestamp '2011-05-16 15:36:38', 1, 'f', 1,1,'f', 1, 1, 1, 1, 1, 'f', 'f','f','f','f')" ,
#    parameters = ( timestamp '2011-05-16 15:36:38', 1, 'f', 1,1,'f', 1, 1, 1, 1, 1, 'f', 'f','f','f','f' ) 
#    trigger_rule=TriggerRule.ALL_DONE,
   # )
    drop_table = PostgresOperator(
        sql='delete from  yandex_pre2;''',
        task_id='drop_table'
    #    provide_context = True,
   #     python_callable=upload_data_to_db,
        )
    insert_table = PythonOperator(
        task_id='append_table',
        provide_context = True,
        python_callable=upload_data_to_db,
        dag=dag,
    )
#    join_table = PostgresOperator(
 #       task_id='join_table',
  #      sql='''insert into all_channels_ga select yandex_pre2.* from yandex_pre2 left join all_channels_ga on all_channels_ga."Ad_id" = yandex_pre2."Ad_id" and all_channels_ga."Date" = yandex_pre2."Date"  and
# all_channels_ga."Campaign_id" = yandex_pre2."Campaign_id" and 
#all_channels_ga."AdGroup_Id" = yandex_pre2."AdGroup_Id" and all_channels_ga."Client_id" = yandex_pre2."Client_id"
#where all_channels_ga."Ad_id" is null and all_channels_ga."Date" is null;''',
#         dag=dag, )
    join_table = PostgresOperator(
        task_id='join_table',
        sql='''insert into all_channels_ga select yandex_pre2.* from yandex_pre2 left join all_channels_ga on all_channels_ga."Ad_id" = yandex_pre2."Ad_id" and all_channels_ga."Date" = yandex_pre2."Date"  
where all_channels_ga."Ad_id" is null or  all_channels_ga."Date" is null;''',
         dag=dag, )
    update_table = PostgresOperator(
        task_id='update_table',
        sql='''UPDATE all_channels_ga SET "Impressions" = yandex_pre2."Impressions","Business_date" = yandex_pre2."Business_date",  "Clicks" = yandex_pre2."Clicks", "Bounces" = yandex_pre2."Bounces",
 "Spend"  = yandex_pre2."Spend", "Sessions" =  yandex_pre2."Sessions",
 "Reach"  = yandex_pre2."Reach"
  FROM   yandex_pre2 where all_channels_ga."Ad_id" = yandex_pre2."Ad_id" and all_channels_ga."Date" = yandex_pre2."Date"  and 
 all_channels_ga."Campaign_id" = yandex_pre2."Campaign_id" and 
all_channels_ga."AdGroup_Id" = yandex_pre2."AdGroup_Id"  ;''',
        dag=dag,    )
    delete_xcom_task = PostgresOperator(
        task_id='delete-xcom-task',
#      postgres_conn_id='airflow_db',
        sql='''delete from xcom where dag_id=dag_id and
           task_id='download_data' ''',
        dag=dag)
    delete_xcom_task2 = PostgresOperator(
        task_id='delete-xcom-task2',
#      postgres_conn_id='airflow_db',
        sql='''delete from xcom where dag_id=dag_id and
           task_id='download_data2' ''',
        dag=dag)
    create_table >> create_table2 >>drop_table  >>  insert_table >> join_table >> update_table >> delete_xcom_task>> delete_xcom_task2