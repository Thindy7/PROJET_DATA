#importer les librairies
import pandas as pd
import textblob 
import pynytimes as NYTAPI
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta


#Analyse de sentiments sur les articles de Obama

def analyze_sentiment(text):
    blob = TextBlob(text)
    return blob.sentiment.polarity

#Fonction pour effectuer la partie extraction des données du pipeline ETL

key = "eujZVG99yNWDCAneFIsuUlxMZIbSAvwF"
def get_extract(key):
    nyt = NYTAPI(key)
    begin_date_str = "20220101" 
    end_date_str = "20220201" 

    begin_date = datetime.strptime(begin_date_str, "%Y%m%d").date() 
    end_date = datetime.strptime(end_date_str, "%Y%m%d").date()

    articles_obam = nyt.article_search(
        query="Obama",
        dates={"begin": begin_date, "end": end_date},
        results=43,
        options={"sort": "newest"}
    )
    return articles_obam


#Fonction pour effectuer la partie tranformation des données du pipeline ETL

def transform_data(articles_obam):
    # Suppression des colonnes inutiles
    articles_obam.drop(['_id', 'document_type', 'multimedia', 'news_desk', 
                        'print_page', 'print_section','section_name', 'snippet', 
                        'type_of_material', 'word_count', 'subsection_name'], axis=1, inplace=True)

    # Renommage des colonnes
    articles_obam.rename(columns={'abstract': 'resume', 'headline.main': 'titre', 
                        'pub_date': 'date', 'web_url': 'url'}, inplace=True)
    return articles_obam


# Fonction pour effectuer la partie chargement des données du pipeline ETL

def load_data(articles_obam):
    # Chargement du fichier articles_obam.csv
    articles_obam.to_csv('articles_obam.csv', index=False)


#Créer des taches pour le pipeline ETL

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 2, 28),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('etl', default_args=default_args, schedule_interval="@daily")

start = DummyOperator(task_id='start', dag=dag)

extract = PythonOperator(
    task_id='extract',
    python_callable=get_extract,
    op_kwargs={'key': key},
    dag=dag
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag
)

load = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

start >> extract >> transform >> load >> end