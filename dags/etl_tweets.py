from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import re
from sqlalchemy import create_engine

POSTGRES_CONN_STR = "postgresql+psycopg2://a2s:3004@postgres:5432/airflow"

def extract(**kwargs):
    src = "/opt/airflow/data/Tweets.csv"
    df = pd.read_csv(src)
    df.to_csv("/opt/airflow/data/extracted.csv", index=False)

def transform(**kwargs):
    df = pd.read_csv("/opt/airflow/data/extracted.csv")
    df["text_clean"] = df["text"].astype(str).str.lower().str.replace(r"[^a-z0-9\\s#@]", "", regex=True)
    def join_hashtags(text):
        if not isinstance(text, str):
            return ""
        tags = re.findall(r"(#\\w+)", text)
        return ",".join(tags)
    df["hashtags"] = df["text_clean"].apply(join_hashtags)
    df.to_csv("/opt/airflow/data/transformed.csv", index=False)

def sentiment_global(**kwargs):
    df = pd.read_csv("/opt/airflow/data/transformed.csv")
    cal=pd.DataFrame({
        "moyen_positive":[(df["airline_sentiment"]=="positive").sum()/len(df)],
        "moyen_negative" :[(df["airline_sentiment"]=="negative").sum()/len(df)]
        })
    cal.to_csv("/opt/airflow/data/cal.csv", index=False)

def load(**kwargs):
    df = pd.read_csv("/opt/airflow/data/transformed.csv")
    engine = create_engine(POSTGRES_CONN_STR)
    df.to_sql("tweets_clean", engine, if_exists="append", index=False)

with DAG(
    dag_id="etl_tweets",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False
) as dag:

    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load", python_callable=load)
    t4 = PythonOperator(task_id="call",python_callable=sentiment_global)

    t1 >> t2 >> [t4, t3]
