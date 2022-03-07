

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas.io.json import json_normalize
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, date, datetime
import pandas as pd
import requests, json, ast
import urllib
import time

#declare DAG parameters:
dag_params = {
    'dag_id': 'PostgresOperator_dag_fb_placement1day',
    'start_date': datetime(2021, 6, 21, hour=8, minute=0, second=0),
    'schedule_interval':  '50 21 * * *',
#    'schedule_interval':  None
}


#declare private credentials
app_id = ''
app_secret = ''
token = ''
business_id=''

#extend access token validity:
long_token_request=requests.get("https://graph.facebook.com/oauth/access_token?grant_type=fb_exchange_token&client_id={}&client_secret={}&fb_exchange_token={}&access_token={}".format(app_id,app_secret,token,token))
lt=long_token_request.text
test_token= json.loads(lt)
new_token=test_token.get('access_token')


token=str(new_token)

#declare Python function for DAG Task 1 (get account_IDs list):
def get_fb_clients_df(**context):
    global requests
    global date
    # Get clients DF:
    list_ad_account_ids=requests.get("https://graph.facebook.com/v11.0/{}/client_ad_accounts?access_token={}&limit=1000".format(business_id,token))
    owned_ad_account_ids=requests.get("https://graph.facebook.com/v11.0/{}/owned_ad_accounts?access_token={}&limit=1000".format(business_id,token))

    
    ad_account_id = pd.concat([json_normalize(list_ad_account_ids.json().get('data')), 
                               json_normalize(owned_ad_account_ids.json().get('data'))]).reset_index(drop=True)['id'].values
    print('AAI',ad_account_id)
    ad_account_id = ad_account_id.tolist()
    try:
        ad_account_id.remove('')
    except:
        pass
    print('AAI',ad_account_id)
    context['ti'].xcom_push(key = 'ad_account_id', value = ad_account_id)
    return ad_account_id


#declare Python function for DAG Task 2 (get full data statystics):
def get_fb_df(**context):

    ad_account_id = context['ti'].xcom_pull(key = 'ad_account_id')
    global pd
    #declare Fields API get request:

    fields= 'account_id,account_name,adset_name,adset_id, campaign_name, campaign_id, ad_id, ad_name,impressions,reach,spend,clicks,objective'#conversions,cpc,cpm,cpp,ctr,frequency,'
    fields1= 'campaign_id'#conversions,cpc,cpm,cpp,ctr,frequency,'
    fields2= 'adset_id'#conversions,cpc,cpm,cpp,ctr,frequency,'
    breakdowns ='publisher_platform, platform_position'


    #generate list of Dates:
    list_of_dates_corr = []
    for stack in range(  1, 2):
            day = str(date.today()-timedelta(days=stack))
            list_of_dates_corr.append(day)
    report = pd.DataFrame()

    err=[]
    ad_account_id_mtsm = ['act_235771224535490','act_928762271231036', 'act_383175462391540']
    print('L:OD___',list_of_dates_corr)
    filtering = [{'field':'ad.effective_status','operator':'IN','value':['ARCHIVED','DELETED']}]
    
    #request statistics data by each client separately (API limits don't allow to get all data with one request):
    for act in ad_account_id_mtsm:
        #request statistics data by each date:
        for date_ in list_of_dates_corr:
            r=requests.get("https://graph.facebook.com/v11.0/{}/insights?access_token={}&time_range=%7B%22since%22%3A%22{}%22%2C%22until%22%3A%22{}%22%7D&fields={}&level=campaign".format(act,token,date_,date_,fields1))
            #download deleted and archieved campaigns:
            r6=requests.get("https://graph.facebook.com/v11.0/{}/insights?access_token={}&time_range=%7B%22since%22%3A%22{}%22%2C%22until%22%3A%22{}%22%7D&fields={}&level=campaign&filtering={}".format(act,token,date_,date_,fields1,filtering))
            cont = True
            df2 = pd.DataFrame()
            #get data using paging (API limits don't allow to get all data with one request)%
            while cont :
                if r.json().get('paging') == None  :
                    cont = False
               
                else:
                    df = json_normalize(r.json(), 'data')
                    df2 = df2.append(df)
                    data_paging = r.json().get('paging')

                    next_ = pd.DataFrame(data_paging['cursors'], index=[0])

                    if 'next' in data_paging.keys():

                        r = requests.get(data_paging['next'])
                    else:
                        cont = False
            df2 = df2.reset_index(drop = True)

            if r6.json().get('data') != None:
                if len(r6.json().get('data')) != 0 :
                    df6 = json_normalize(r6.json(), 'data')

                    campaign_ids6 = set(df6['campaign_id'])

            print(date_, '_', r.headers, '_', 'status_code',r.status_code)

            print(r.text)
            if r.json().get('data') != None:
                if len(r.json().get('data')) != 0 :

                    df = json_normalize(r.json(), 'data')
                    print(df)
                    campaign_ids = set(df2['campaign_id'])
                    print('IDDDDS', campaign_ids)
                    print('!',act)
                    err.append(act)
                    #iterate through campaign_IDs:
                    if campaign_ids:
                        report_ = pd.DataFrame()
                        for campaign in campaign_ids:
                            #try:
                            r2=requests.get("https://graph.facebook.com/v11.0/{}/insights?access_token={}&time_range=%7B%22since%22%3A%22{}%22%2C%22until%22%3A%22{}%22%7D&fields={}&level=adset".format(campaign,token,date_,date_,fields2))
                            time.sleep(1)

                            r3=requests.get("https://graph.facebook.com/v11.0/{}/insights?access_token={}&time_range=%7B%22since%22%3A%22{}%22%2C%22until%22%3A%22{}%22%7D&fields={}&level=adset&filtering={}".format(campaign,token,date_,date_,fields2,filtering))

                            if len(r2.json().get('data')) != 0 and r2.json().get('data') != None:

                                df2 = json_normalize(r2.json(), 'data')
                                report_ = report_.append(df2, sort=False)

                            if len(r3.json().get('data')) != 0 and r3.json().get('data') != None:

                                df3 = json_normalize(r3.json(), 'data')
                                report_ = report_.append(df3, sort=False)

                            err.append(act)

                            adsets = set(report_['adset_id'])
                            #iterate through adsets to get full data statystics by Ad_ID:

                            for adset in adsets:

                                r4=requests.get("https://graph.facebook.com/v11.0/{}/insights?access_token={}&time_range=%7B%22since%22%3A%22{}%22%2C%22until%22%3A%22{}%22%7D&fields={}&level=ad&breakdowns={}".format(adset,token,date_,date_,fields, breakdowns))

                                print('R4',r4, type(r4))
                                r5=requests.get("https://graph.facebook.com/v11.0/{}/insights?access_token={}&time_range=%7B%22since%22%3A%22{}%22%2C%22until%22%3A%22{}%22%7D&fields={}&level=ad&filtering={}&breakdowns={}".format(adset,token,date_,date_,fields,filtering, breakdowns))

                                i+=1
                                print('IIIII', i)

                                if  r4.json().get('data') != None :
                                    cont50 = True
                                    df50  = pd.DataFrame()
                                    while cont50:
                                        if r4.json().get('paging') == None  :
                                            cont50 = False
                                        else:
                                            df51 = json_normalize(r4.json(), 'data')
                                            df50 = df50.append(df51)
                                            data_paging50 = r4.json().get('paging')
                                            next_50 = pd.DataFrame(data_paging50['cursors'], index=[0])
                                            if 'next' in data_paging50.keys():
                                                r4 = requests.get(data_paging50['next'])
                                            else:
                                                cont50 = False
                                    df50 = df50.reset_index(drop = True)
                                    print(set(df50['ad_id']))
                                    report = report.append(df50, sort = False)

                                if   r5.json().get('data') != None and len(r5.json().get('data')) != 0:

                                    df5 = json_normalize(r5.json(), 'data')
                                    print(df5)
                                    print(set(df5['ad_id']))
                                    report = report.append(df5, sort=False)

                            time.sleep(1)


    report.rename(columns={
        'account_id':'Client_id',
        'account_name':'Client_name',
        'adset_name':'AdGroup_Name',
        'adset_id':'AdGroup_Id',
        'date_start':'Date',
        'ad_id':'Ad_id',
        'ad_name':'Ad_name',
        'impressions':'Impressions',
        'clicks':'Clicks',
        'reach':'Reach',
        'campaign_name':'Campaign_name',
        'campaign_id':'Campaign_id',
        'spend':'Spend',
        'bounces':'Bounces',
        'sessions':'Sessions'},inplace=True)
    report['Date'] =  pd.to_datetime(report['Date'], errors='coerce').dt.date
    report['Business_date'] = datetime.now().replace(microsecond=0)
    report['Business_date'] =  pd.to_datetime(report['Business_date'], errors='coerce')
    report['Platform']='FB'

    report.drop(['date_stop'],axis=1,inplace=True)

    print(report)
    report = report.drop_duplicates()
    context['ti'].xcom_push(key = 'clients_fb_df', value = report)

    return report


engine =''

# declare Python function for DAG Task 3 (upload data to local PostgreSQL Database):
def upload_fb_data_to_db(**context):
    global engine
    table = context['ti'].xcom_pull(key = 'clients_fb_df') 
    table.to_sql('fb_pre_placement', engine, if_exists='append', index = False)

#define subsequence of DAG Tasks:
with DAG(**dag_params) as dag:

    create_table_fb_new = PythonOperator(
        task_id='download_data_fb_new',
        provide_context = True,
        python_callable=get_fb_clients_df,
        dag=dag,
    )

    create_table2_fb_new  = PythonOperator(
        task_id='download_data2_fb_new',
        python_callable=get_fb_df,
        dag=dag,
        provide_context = True,
    )
    push_extra_date = PythonOperator(
        task_id = 'push_extra_date_fb_placement',
        python_callable=push_extra_date,
        dag=dag,
        provide_context = True,    
    )

    drop_table_fb = PostgresOperator(
        sql='delete from  fb_pre_placement;''',
        task_id='drop_table_fb'

    )
    insert_table_fb = PythonOperator(
        task_id='append_table_fb',
        provide_context = True,
        python_callable=upload_fb_data_to_db,
        dag=dag,
    )
    delete_from_all_ch = PostgresOperator(
        task_id='delete_from_all_ch',
        sql = '''delete from all_channels_ga_fb_placement where "Date" in (select distinct "Date" from fb_pre_placement) ;''',
        dag=dag, )
    join_table_fb = PostgresOperator(
        task_id='join_table_fb',
        sql = ''' insert into all_channels_ga_fb_placement select * from fb_pre_placement ;''',
        dag=dag, )


    delete_xcom_task_fb = PostgresOperator(
        task_id='delete-xcom-task_fb',
        sql='''delete from xcom where dag_id=dag_id and
           task_id='download_data_fb_new' ''',
        dag=dag)
    delete_xcom_task2_fb = PostgresOperator(
        task_id='delete-xcom-task2_fb',
        sql='''delete from xcom where dag_id=dag_id and
           task_id='download_data2_fb_new' ''',
        dag=dag)

    create_table_fb_new  >> create_table2_fb_new   >>push_extra_date>>delete_xcom_old_date_fb>>push_new_date>>delete_xcom_extra_date_fb >>drop_table_fb>>  insert_table_fb >>delete_from_all_ch>> join_table_fb >> delete_xcom_task_fb >> delete_xcom_task2_fb