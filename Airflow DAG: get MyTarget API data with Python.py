from sqlalchemy import create_engine
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests, json
from requests.exceptions import ConnectionError
from time import sleep
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from pandas.io.json import json_normalize
from datetime import datetime, date, timedelta
import pandas as pd
import requests, json, ast
import time

dag_params = {
    'dag_id': 'PostgresOperator_dag_mt_new',
 
    'start_date': datetime(2021, 3, 26, hour=18, minute=0, second=0),
    'schedule_interval':  '10-59/30 * * * *'
}

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#declare private credentials:
token = ''
client_id = ''
client_secret = ''

#date range:
fom = 10
to = 0

def get_mt_clients(**context):
    global token
    global requests
    # получаем выгрузку всех клиентов, цикл нужен чтобы избежать ограничения в 50 объектов
    cont = True
    offset=0
    df_clients = pd.DataFrame()
    while cont:
        clients = requests.get('https://target.my.com/api/v2/agency/clients.json?limit=50&offset={}'.format(offset), headers={'Authorization': 'Bearer {}'.format(token)})
        offset = offset + 50
        print(clients.json().get('items'), 'print')
        if len(clients.json().get('items')) == 0:
            cont=False
        df_clients = pd.concat([df_clients, json_normalize(clients.json(), 'items')])
    df_clients = df_clients.reset_index(drop=True)
    
    for client in df_clients['user.username'].values:
        url = 'https://target.my.com/api/v2/oauth2/token/delete.json'
        myobj={
        'client_id': client_id,
        'client_secret' : client_secret,
        'username': client
        }
        response = requests.post(url, data = myobj)
    tokens=[]
    
    for client in df_clients['user.username'].values:
        url = 'https://target.my.com/api/v2/oauth2/token.json'
        myobj={
        'grant_type': 'agency_client_credentials',
        'client_id': client_id,
        'client_secret' : client_secret,
        'agency_client_name': client
            }
        response = requests.post(url, data = myobj)
        tok = response.json().get('access_token')
        if tok != None:
            tokens.append(tok)
            df_clients.loc[df_clients['user.username'] == client, 'token'] = tok
    context['ti'].xcom_push(key = 'mt_tokens', value = tokens)
    context['ti'].xcom_push(key = 'df_clients_mt', value = df_clients)
    return tokens

def get_mt_banners(**context): 
    # получаем список всех баннеров
    tokens = context['ti'].xcom_pull(key = 'mt_tokens')
    df_clients = context['ti'].xcom_pull(key = 'df_clients_mt')
    global pd
    global requests
#    df_banners = pd.DataFrame()
 #   for token in tokens:    
  #      cont = True
   #     offset=0

    #    while cont:
     #       banners = requests.get('https://target.my.com/api/v2/banners.json?limit=50&offset={}'.format(offset), headers={'Authorization': 'Bearer {}'.format(token)})
      #      offset = offset + 50
       #     try:
        #        if len(banners.json().get('items')) == 0:
         #           cont=False
          #      df_banners = pd.concat([df_banners, json_normalize(banners.json(), 'items')])
           # except:
            #    continue
    # получаем список всех баннеров
    #df_banners = pd.DataFrame()

    def func2(tokens):
        df_banners = pd.DataFrame()
        #for token in tokens:    

            #offset=0
        def func(token_, offset,cont_,df_banners2):
            df_banners3 = df_banners2
            offset_2=offset
            cont2 = cont_
            #pdb.set_trace()
            #def func3(offset):
            while cont2 :
                banners = requests.get('https://target.my.com/api/v2/banners.json?limit=50&offset={}'.format(offset_2), headers={'Authorization': 'Bearer {}'.format(token_)})

               # print( '_', banners.headers, '_', 'status_code',banners.status_code)
                print( '_',  '_', 'status_code',banners.status_code)
                if banners.status_code == 200:
                    print('yeeeeaahhh')
                    #time.sleep(0.3)
                    offset_2 = offset_2 + 50

                    if banners.json().get('items') is None or len(banners.json().get('items')) == 0:
                        print('gh ', banners.json())

                        cont2=False
                    else:

                        hj = json_normalize(banners.json(), 'items')
                        print(hj.shape)
                        print(banners.json())
                        df_banners3 = df_banners3.append(hj)
                        time.sleep(1)
                elif banners.status_code == 429:

                    time.sleep(1)
                    df_banners3 = func(token, offset_2,cont2,df_banners3)
                    #df_banners3=df_banners3.append(b)
            return df_banners3
        for token in tokens:
            h = func(token, 0, True,pd.DataFrame())
            df_banners = df_banners.append(h)
        return df_banners

                #df_banners = pd.concat([df_banners, json_normalize(banners.json(), 'items')])
            #except:
             #   continue
    df_banners = func2(tokens)
    df_banners = df_banners.drop_duplicates()
    # получаем список всех кампаний
    #df_banners = pd.DataFrame()

    def func2_c(tokens):
        df_banners = pd.DataFrame()
        #for token in tokens:    

            #offset=0
        def func_c(token_, offset,cont_,df_banners2):
            df_banners3 = df_banners2
            offset_2=offset
            cont2 = cont_
            #pdb.set_trace()
            #def func3(offset):
            while cont2 :
                banners = requests.get('https://target.my.com/api/v2/campaigns.json?limit=50&offset={}'.format(offset_2), headers={'Authorization': 'Bearer {}'.format(token_)})

               # print( '_', banners.headers, '_', 'status_code',banners.status_code)
                print( '_',  '_', 'status_code',banners.status_code)
                if banners.status_code == 200:
                    print('yeeeeaahhh')
                    #time.sleep(0.3)
                    offset_2 = offset_2 + 50

                    if banners.json().get('items') is None or len(banners.json().get('items')) == 0:
                        print('gh ', banners.json())

                        cont2=False
                    else:

                        hj = json_normalize(banners.json(), 'items')
                        print(hj.shape)
                        print(banners.json())
                        df_banners3 = df_banners3.append(hj)
                        time.sleep(1)
                elif banners.status_code == 429:

                    time.sleep(1)
                    df_banners3 = func(token, offset_2,cont2,df_banners3)
                    #df_banners3=df_banners3.append(b)
            return df_banners3
        for token in tokens:
            h = func_c(token, 0, True,pd.DataFrame())
            df_banners = df_banners.append(h)
        return df_banners

                #df_banners = pd.concat([df_banners, json_normalize(banners.json(), 'items')])
            #except:
             #   continue

    # выгрузка всех кампаний
    df_campaigns = func2_c(tokens)
    df_campaigns = df_campaigns.drop_duplicates()
    print('shapig', df_banners.shape, df_campaigns.shape )
#    for token in tokens:
 #       cont = True
  #      offset=0
   #     while cont:
    #        response = requests.get('https://target.my.com/api/v2/campaigns.json?limit=50&offset={}'.format(offset), headers={'Authorization': 'Bearer {}'.format(token)})
     #       offset = offset + 50
      #      try:
       #         if len(banners.json().get('items')) == 0:
        #            cont=False
         #       df_campaigns = pd.concat([df_campaigns, json_normalize(response.json(), 'items')])
          #  except:
           #     continue
        # объединяем баннеры и кампании
    df_campaigns.columns=['campaign_id', 'name', 'package_id']
    df_banners = df_banners.merge(df_campaigns, on='campaign_id', how='left')[['name', 'campaign_id', 'id']]
    df_banners.columns = ['campaign_name', 'campaign_id', 'id']

    date1 = (datetime.today() - timedelta(days=fom)).strftime("%Y-%m-%d")
    date2 = (datetime.today() - timedelta(days=to)).strftime("%Y-%m-%d")
        # получаем список всех баннеров
    #df_banners = pd.DataFrame()

    # получаем список всех баннеров
    #df_banners = pd.DataFrame()

    def func2_users(tokens, date_from, date_to):

        report = pd.DataFrame()
        #for token in tokens:    

            #offset=0
        def func_users(token_, from_, to_, df_stats):
            report = df_stats
            #offset_2=offset
            #cont2 = cont_
            #pdb.set_trace()
            #def func3(offset):

#            for day in range(fom,to,-1):
#            print(day)
#            date = (datetime.today() - timedelta(days=day)).strftime("%Y-%m-%d")
            response = requests.get('https://target.my.com/api/v2/statistics/users/day.json?date_from={}&date_to={}&metrics=base'.format(from_,to_), headers={'Authorization': 'Bearer {}'.format(token_)})
                #try:
            print(response.headers, ' status_code ', response.status_code)
            if response.status_code == 200:
                print('yeeeeaahhh')
                if response.json().get('items') is not None or len(response.json().get('items')) != 0  :

                    df = json_normalize(response.json(), 'items')#.join(json_normalize(response.json(), ['items']))
                    df['token'] = token
                    report = report.append(df)
                    time.sleep(1)
            elif response.status_code == 429:
                time.sleep(1)
                report = func_users(token_, from_, to_, report)
                        #df_banners3=df_banners3.append(b)
            return report
    #         date1 = (datetime.today() - timedelta(days=fom)).strftime("%Y-%m-%d")
    #         date2 = (datetime.today() - timedelta(days=to)).strftime("%Y-%m-%d")
    #         response = requests.get('https://target.my.com/api/v2/statistics/banners/day.json?date_from={}&date_to={}&metrics=base'.format(date1,date2), headers={'Authorization': 'Bearer {}'.format(token_)})
    #         #try:
    #         print(response.headers, ' status_code ', response.status_code)
    #         if response.status_code == 200:
    #             print('yeeeeaahhh')
    #             if response.json().get('items') is not None or len(response.json().get('items')) != 0  :

    #                 df = json_normalize(response.json(), 'items').join(json_normalize(response.json(), ['items','rows']))
    #                 df['token'] = token
    #                 report = report.append(df)
    #         elif response.status_code == 429:

    #                 time.sleep(0.5)
    #                 report = func_campaigns(token_, fom, to, report)
    #                 #df_banners3=df_banners3.append(b)
    #         return report


        for token in tokens:
            print(token)
            h = func_users(token, date_from, date_to, pd.DataFrame())
            report = report.append(h)
        return report

    users = func2_users(tokens, date1, date2)
    users_not_empty = set(users['token'][users['total.base.shows'] != 0])
    print('UNEEEEEE',users_not_empty)
    def func2_campaigns(tokens, date_from, date_to):

        report = pd.DataFrame()
        #for token in tokens:    

            #offset=0
        def func_campaigns(token_, fom, to, df_stats):
            report = df_stats
            #offset_2=offset
            #cont2 = cont_
            #pdb.set_trace()
            #def func3(offset):

            for day in range(fom,to,-1):
                print(day)
                date = (datetime.today() - timedelta(days=day)).strftime("%Y-%m-%d")
                response = requests.get('https://target.my.com/api/v2/statistics/banners/day.json?date_from={}&date_to={}&metrics=base'.format(date,date), headers={'Authorization': 'Bearer {}'.format(token_)})
                #try:
                print(response.headers, ' status_code ', response.status_code)
                if response.status_code == 200:
                    print('yeeeeaahhh')
                    if response.json().get('items') is not None or len(response.json().get('items')) != 0  :

                        df = json_normalize(response.json(), 'items').join(json_normalize(response.json(), ['items','rows']))
                        df['token'] = token
                        report = report.append(df)
                        time.sleep(1)
                elif response.status_code == 429:

                    time.sleep(1)
                    report = func_campaigns(token_, fom, to, report)
                      #df_banners3=df_banners3.append(b)
            return report

    #         date1 = (datetime.today() - timedelta(days=fom)).strftime("%Y-%m-%d")
    #         date2 = (datetime.today() - timedelta(days=to)).strftime("%Y-%m-%d")
    #         response = requests.get('https://target.my.com/api/v2/statistics/banners/day.json?date_from={}&date_to={}&metrics=base'.format(date1,date2), headers={'Authorization': 'Bearer {}'.format(token_)})
    #         #try:
    #         print(response.headers, ' status_code ', response.status_code)
    #         if response.status_code == 200:
    #             print('yeeeeaahhh')
    #             if response.json().get('items') is not None or len(response.json().get('items')) != 0  :

    #                 df = json_normalize(response.json(), 'items').join(json_normalize(response.json(), ['items','rows']))
    #                 df['token'] = token
    #                 report = report.append(df)
    #         elif response.status_code == 429:

    #                 time.sleep(0.5)
    #                 report = func_campaigns(token_, fom, to, report)
    #                 #df_banners3=df_banners3.append(b)
    #         return report


        for token in tokens:
            print(token)
            h = func_campaigns(token, date_from, date_to, pd.DataFrame())
            report = report.append(h)
        return report
    report = func2_campaigns(users_not_empty, fom, to)
    # делаем выгрузку за несколько дней
 #   report = pd.DataFrame()
  #  for token in tokens:
   #     for day in range(fom,to,-1):
    #        date = (datetime.today() - timedelta(days=day)).strftime("%Y-%m-%d")
     #       response = requests.get('https://target.my.com/api/v2/statistics/banners/day.json?date_from={}&date_to={}&metrics=base'.format(date,date), headers={'Authorization': 'Bearer {}'.format(token)})
      #      try:
       #         df = json_normalize(response.json(), 'items').join(json_normalize(response.json(), ['items','rows']))
        #        df['token'] = token
         #       report = report.append(df)
          #  except:
           #     continue
        # добавляем в выгрузку имя клиента и название кампаний
    cols = [i for i in report.columns if "total" not in i]
    report = report[cols].drop('rows', axis=1)
    MT_repor = report.reset_index(drop=True) 
    MT_repor = MT_repor.merge(df_clients[['user.client_username', 'user.id', 'token']], on='token', how='left')
    MT_repor = MT_repor.merge(df_banners, on='id', how='left')
    MT_repor = MT_repor.drop('token', axis=1)
    
    MT_repor.rename(columns={
    'user.id':'Client_id',
    'date':'Date',
    'id':'Ad_id',
    'base.shows':'Impressions',
    'base.clicks':'Clicks',
    'campaign_name':'Campaign_name',
    'campaign_id':'Campaign_id',
    'base.spent':'Spend',
    'user.client_username':'Client_name',
    'sessions':'Sessions'},inplace=True)
    MT_repor['Date'] =  pd.to_datetime(MT_repor['Date'], errors='coerce').dt.date
    MT_repor['Business_date'] = datetime.now().replace(microsecond=0)
    MT_repor['Business_date'] =  pd.to_datetime(MT_repor['Business_date'], errors='coerce')
    MT_repor['Platform']='MT'
    MT_repor['Sessions']=None
    MT_repor['Bounces']=None
    MT_repor['Reach']=None
    MT_repor['AdGroup_Id']=None
    MT_repor['AdGroup_Name']=None
    MT_repor['Ad_name']=None
    MT_repor.drop(['base.goals','base.cpm','base.cpc', 'base.cpa', 'base.ctr', 'base.cr'],axis=1,inplace=True)
    MT_repor['index'] = None
    MT_repor['medium'] = None
    MT_repor['Business_date_GA'] = None
    MT_repor['keyword'] = None
    MT_repor['campaign_GA'] = None
    MT_repor['date_GA'] = None
    MT_repor['sourceMedium'] = None
    MT_repor['adContent'] = None
    MT_repor['source'] = None
    MT_repor['goal1Completions'] = None
    MT_repor['sessions_GA'] = None
    MT_repor['goal2Completions'] = None
    MT_repor['impressions_GA'] = None
    MT_repor['sessionDuration_GA'] = None
    MT_repor['click_GA'] = None
    MT_repor['goal3Completions'] = None
    MT_repor['adCost'] = None
    MT_repor['bounces_GA'] = None
    MT_repor['adContent - for YA'] = None
    MT_repor['adContent - for FB'] = None
   # MT_repor['Ad_id'] = MT_repor['Ad_id'].fillna(0)
    MT_repor['Ad_id']= MT_repor['Ad_id'].astype(int)
   # MT_repor['Ad_id']= MT_repor['Ad_id'].astype(str)
   # MT_repor['Ad_id'][MT_repor['Ad_id'] == '0']= None
   # MT_repor['Campaign_id'] = MT_repor['Campaign_id'].fillna(0)
    MT_repor['Campaign_id']= MT_repor['Campaign_id'].astype(int)
  #  MT_repor['Campaign_id']= MT_repor['Campaign_id'].astype(str)
   # MT_repor['Campaign_id'][MT_repor['Campaign_id'] == '0']= None
    MT_repor['1'] = None
    MT_repor['2'] = None
    MT_repor['3'] = None
    MT_repor['4'] = None
    MT_repor['5'] = None
    MT_repor['6'] = None
    MT_repor['7'] = None
    MT_repor['8'] = None
    MT_repor['9'] = None
    MT_repor['10'] = None
    MT_repor['11'] = None
    MT_repor['12'] = None
    MT_repor['13'] = None
    MT_repor['14'] = None
    MT_repor['15'] = None
    MT_repor['16'] = None
    MT_repor['17'] = None
    MT_repor['18'] = None
    MT_repor['19'] = None
    MT_repor['20'] = None
    MT_repor = MT_repor.drop_duplicates()
    context['ti'].xcom_push(key = 'clients_df_mt', value = MT_repor) 
    return MT_repor

engine ='postgresql+psycopg2://airflow@localhost:8123/digitaladsdb'
def upload_mt_data_to_db(**context):
#    postgres_hook = PostgresHook(self.postgres_conn_id)
    global engine
#engine = postgres_hook.get_sqlalchemy_engine()
    table = context['ti'].xcom_pull(key = 'clients_df_mt') 
   # engine = create_engine('postgresql://airflow:y59RaFJKqy@192.168.127.16:8123/digitalads')
    table.to_sql('mt_pre2', engine, if_exists='append', index = False)
    
with DAG(**dag_params) as dag:

    create_table_mt = PythonOperator(
        task_id='download_data_mt',
        provide_context = True,
        python_callable=get_mt_clients,
        dag=dag,
    )
#    print(type(create_table))    
#with DAG(**dag_params) as dag:
    create_table2_mt = PythonOperator(
        task_id='download_data2_mt',
        python_callable=get_mt_banners,
        dag=dag,
        provide_context = True,
    )
    drop_table_mt = PostgresOperator(
        sql='delete from mt_pre2;''',
        task_id='drop_table_mt'
    #    provide_context = True,
   #     python_callable=upload_data_to_db,
        )
    insert_table_mt = PythonOperator(
        task_id='append_table_mt',
        provide_context = True,
        python_callable=upload_mt_data_to_db,
        dag=dag,
    )
#    join_table_mt = PostgresOperator(
 #       task_id='join_table_mt',
  #      sql='''insert into all_channels_ga ("Ad_id", "Date", "Campaign_id", "Campaign_name",  "Impressions", "Clicks",  "Platform", "Spend", "Client_name", "Client_id")
#select "Ad_id", "Date", "Campaign_id", "Campaign_name",  "Impressions", "Clicks",  "Platform", "Spend", "Client_name", "Client_id" from mt_pre2
#where "Ad_id"  not in (select distinct "Ad_id" from all_channels_ga where "Platform" = 'MT') 
#or "Date"  not in (select distinct "Date" from all_channels_ga where "Platform" = 'MT');''',
 #        dag=dag, )
  #  update_table_mt = PostgresOperator(
   #     task_id='update_table_mt',
    #    sql='''UPDATE all_channels_ga SET "Impressions" = mt_pre2."Impressions", "Business_date" = mt_pre2."Business_date", "Clicks" = mt_pre2."Clicks", "Bounces" = mt_pre2."Bounces", "Spend" = mt_pre2."Spend", "Sessions" = mt_pre2."Sessions",
# "Reach" =  mt_pre2."Reach"
 # FROM   mt_pre2 where all_channels_ga."Ad_id" = mt_pre2."Ad_id" and all_channels_ga."Date" = mt_pre2."Date"    ;''',
  #      dag=dag, )
    join_table_mt = PostgresOperator(
        task_id='join_table_mt',
        sql='''insert into all_channels_ga select mt_pre2.* from mt_pre2 left join all_channels_ga on all_channels_ga."Ad_id" = mt_pre2."Ad_id" and all_channels_ga."Date" = mt_pre2."Date"  
where all_channels_ga."Ad_id" is null or  all_channels_ga."Date" is null;''',
         dag=dag, )
    update_table_mt = PostgresOperator(
        task_id='update_table_mt',
        sql='''UPDATE all_channels_ga SET "Impressions" = mt_pre2."Impressions","Business_date" = mt_pre2."Business_date",  "Clicks" = mt_pre2."Clicks", "Bounces" = mt_pre2."Bounces",
 "Spend"  = mt_pre2."Spend", "Sessions" =  mt_pre2."Sessions",
 "Reach"  = mt_pre2."Reach"
  FROM   mt_pre2 where all_channels_ga."Ad_id" = mt_pre2."Ad_id" and all_channels_ga."Date" = mt_pre2."Date"  and 
 all_channels_ga."Campaign_id" = mt_pre2."Campaign_id"
;''', dag=dag, )

    delete_xcom_task_mt = PostgresOperator(
        task_id='delete-xcom-task_mt',
#      postgres_conn_id='airflow_db',
        sql='''delete from xcom where dag_id=dag_id and
           task_id='download_data_mt' ''',
        dag=dag)
    delete_xcom_task2_mt = PostgresOperator(
        task_id='delete-xcom-task2_mt',
#      postgres_conn_id='airflow_db',
        sql='''delete from xcom where dag_id=dag_id and
           task_id='download_data2_mt' ''',
        dag=dag)

    create_table_mt >> create_table2_mt >> drop_table_mt  >>  insert_table_mt>>  join_table_mt  >> update_table_mt  >> delete_xcom_task_mt>> delete_xcom_task2_mt