import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import requests
from bs4 import BeautifulSoup
import sys
from datetime import datetime, date, timedelta

import sqlalchemy 
from sqlalchemy import create_engine

import pandas as pd

def extract_data(**kwargs):

    df = pd.DataFrame(columns=['moeda','valor', 'created_at'])

    url     = 'https://www.infomoney.com.br/mercados/cambio'
    dt_run  = datetime.now()

    page    = requests.get(url)
    soup    = BeautifulSoup(page.text, 'html.parser')
    table   = soup.find_all("table", class_="table-general")[0]

    for tr in table.find_all('tr')[1:]:
        tds = tr.find_all('td')

        df_row = pd.DataFrame({'moeda': [tds[0].text], 'valor': [tds[2].text], 'created_at': [dt_run]})

        df = df.append(df_row, ignore_index = True)

    return df

def transform_data(**kwargs):

    ti = kwargs['ti']
    df = ti.xcom_pull(key=None, task_ids='extract_data')

    df['moeda'] = df['moeda'].apply(clean_currency_name)

    df = df[df.moeda != 'Iene']
    df = df[df.moeda != 'Dólar Turismo']
    df = df[df.moeda != 'Dólar PTAX800']

    df['valor'] = df['valor'].apply(convert_to_float)

    return df


def load_data(**kwargs):

    ti = kwargs['ti']
    df = ti.xcom_pull(key=None, task_ids='transform_data')

    con_str = "postgresql://usuario:senha@localhost/financeiro"
    engine  = create_engine(con_str, pool_size=30, max_overflow=40)
    conn = engine.connect()

    result  = df.to_sql("cotacao", conn, if_exists="append", index=False)

    return result


def clean_currency_name(currency_name):
    return currency_name.strip()

def convert_to_float(currency_value):
    return float(currency_value.replace(",", "."))

default_args = {
    'start_date': dt.datetime(2019, 7, 5),
    'provide_context': True,
    'email': ['arthur@cquantt.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

#####################################################################
# DAG
#####################################################################

with DAG("etl_crawler_v0", default_args=default_args, schedule_interval="@hourly") as dag:
    opr_extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data, dag=dag)
    opr_transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)
    opr_load_data = PythonOperator(task_id='load_data', python_callable=load_data, dag=dag)

opr_extract_data >> opr_transform_data >> opr_load_data



