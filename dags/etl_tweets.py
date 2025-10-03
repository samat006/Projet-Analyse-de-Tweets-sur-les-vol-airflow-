from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import re
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import pandas as pd
import folium


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
        "moyen_negative" :[(df["airline_sentiment"]=="negative").sum()/len(df)],
        "moyen_neutre" :[(df["airline_sentiment"]=="neutral").sum()/len(df)]

        })
    cal.to_csv("/opt/airflow/data/cal.csv", index=False)
    labels = ['Positif', 'Négatif', 'Neutre']
    values = [cal['moyen_positive'][0], cal['moyen_negative'][0], cal['moyen_neutre'][0]]
    plt.figure(figsize=(6,4))
    plt.pie(values,labels=labels,autopct= '%1.1f%%' ,colors=['green', 'red', 'gray'])
    plt.title("Répartition des sentiments")
    output_path = "/opt/airflow/data/sentiment_pie.png"
    plt.savefig(output_path)

    plt.close() 

    # Bar chart
    plt.figure(figsize=(6,4))
    plt.bar(labels, values, color=['green', 'red', 'gray'])
    plt.ylabel("Proportion")
    plt.title("Proportion des sentiments")
    output_path = "/opt/airflow/data/sentiment_bar.png"
    plt.savefig(output_path)

    plt.close() 

  


def map_tweets_world(**kwargs):
    df = pd.read_csv("/opt/airflow/data/transformed.csv")
    df = df.dropna(subset=["tweet_coord"])
    df["lat"] = df["tweet_coord"].apply(lambda x: float(x.strip("[]").split(",")[0]))
    df["lon"] = df["tweet_coord"].apply(lambda x: float(x.strip("[]").split(",")[1]))

    # Carte centrée sur le monde
    m = folium.Map(location=[20, 0], zoom_start=2, tiles="OpenStreetMap")

    # Couleur selon sentiment
    def sentiment_color(sentiment):
        if sentiment == "negative":
            return "red"   # négatif
        elif sentiment == "positive":
            return "green"     # positif
        else:
            return "gray"    # neutre

    # Ajout des points
    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=4,
            popup=f"Sentiment: {row['airline_sentiment']}<br>Tweet: {row['text']}",
            color=sentiment_color(row["airline_sentiment"]),
            fill=True,
            fill_opacity=0.7
        ).add_to(m)

    # Légende personnalisée
    legend_html = """
     <div style="position: fixed; 
                 bottom: 50px; left: 50px; width: 150px; height: 120px; 
                 background-color: white; border:2px solid grey; z-index:9999; font-size:14px;">
     &nbsp;<b>Légende</b><br>
     &nbsp;<i style="color:green;">●</i> Positif <br>
     &nbsp;<i style="color:red;">●</i> Négatif <br>
     &nbsp;<i style="color:gray;">●</i> Neutre <br>
      </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # Sauvegarde
    output_path = "/opt/airflow/data/tweet_map_world.html"
    m.save(output_path)
    print(f"🌍 Carte du monde sauvegardée dans {output_path}")



def load(**kwargs):
    # Charger les tweets transformés
    df = pd.read_csv("/opt/airflow/data/transformed.csv")

    # Charger les stats calculées
    cal = pd.read_csv("/opt/airflow/data/cal.csv")

    # Connexion à PostgreSQL
    engine = create_engine(POSTGRES_CONN_STR)

    # Charger dans la table des tweets nettoyés
    df.to_sql("tweets_clean", engine, if_exists="append", index=False)

    # Charger dans la table des statistiques
    cal.to_sql("statistique", engine, if_exists="replace", index=False)

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
    t5 = PythonOperator(task_id="map_tweets_world",python_callable=map_tweets_world)

    t1 >> t2 >> [t4, t3, t5]


