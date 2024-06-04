from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.sensors import ExternalTaskSensor
from transformers import pipeline
import pandas as pd
from io import StringIO
import boto3

AWS_ACCESS_KEY_ID = "YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "YOUR_AWS_SECRET_ACCESS_KEY"
BUCKET_NAME = "YOUR_S3_BUCKET_NAME"
FILE_NAME = "comments.csv"

def sentiment_analysis(comment):
    sentiment = pipeline(model="lxyuan/distilbert-base-multilingual-cased-sentiments-student",
                         return_all_scores=True)
    output = sentiment(comment)
    sentiment_result = max(output[0], key=lambda x: x['score'])
    label = sentiment_result['label']
    score = sentiment_result['score']
    return label, score

def process_comments_and_sentiment_analysis():
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    s3 = session.client('s3')
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
    df = pd.read_csv(obj['Body'])

    sentiment = pipeline(model="lxyuan/distilbert-base-multilingual-cased-sentiments-student",
                         return_all_scores=True)

    for i, comment in enumerate(df['comment']):
        label, score = sentiment(comment)
        sentiment_result = max(label, key=lambda x: x['score'])
        df.loc[i, 'sentiment'] = sentiment_result['label']
        df.loc[i, 'score'] = sentiment_result['score']

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    return csv_buffer.getvalue(), df

def load_df_to_s3(**context):
    csv_buffer, _ = context['task_instance'].xcom_pull(task_ids='process_comments_and_sentiment_analysis')
    try:
        s3_resource = boto3.resource(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        s3_resource.Object(BUCKET_NAME, "sentiment_analysis_results.csv").put(Body=csv_buffer)
    except Exception as e:
        print(f"AWS access not allowed: {e}")

    return "Data loaded successfully"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'sentiment_analysis_and_load_to_s3',
    default_args=default_args,
    description='Perform sentiment analysis on comments and load results to S3',
    schedule_interval='@daily',
)

wait_extract_comments = ExternalTaskSensor(
    task_id='wait_extract_comments',
    external_dag_id='youtube_comments_extraction',  # Assuming this is the correct DAG ID for the extraction process
    external_task_id='extract_comments',
    timeout=2000,
    dag=dag,
    mode='reschedule',
    allowed_states=["success"])

process_comments_and_sentiment_analysis_task = PythonOperator(
    task_id='process_comments_and_sentiment_analysis',
    python_callable=process_comments_and_sentiment_analysis,
    dag=dag,
)

load_df_to_s3_task = PythonOperator(
    task_id='load_df_to_s3',
    python_callable=load_df_to_s3,
    provide_context=True,
    dag=dag,
)

wait_extract_comments >> process_comments_and_sentiment_analysis_task >> load_df_to_s3_task
