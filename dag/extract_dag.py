from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from googleapiclient.discovery import build
import pandas as pd
import re
import emoji
import boto3
from io import StringIO
from datetime import datetime

API_KEY = "YOUR_API_KEY"
AWS_ACCESS_KEY_ID = "YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "YOUR_AWS_SECRET_ACCESS_KEY"
BUCKET_NAME = "YOUR_S3_BUCKET_NAME"

def extract_comments():
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    URL = "YOUR_VIDEO_URL"
    video_id = URL.split("v=")[-1]  # Extract video ID from URL
    
    comments = []
    next_page_token = None
    
    while len(comments) < 500:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token
            )
            response = request.execute()
            
            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']
                public = item['snippet']['isPublic']
                comments.append([
                    comment['authorDisplayName'],
                    comment['textOriginal'],
                    public
                ])
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        except Exception as e:
            print(f"An error occurred: {e}")
            break

    df = pd.DataFrame(comments, columns=['author', 'comment', 'public'])

    hyperlink_pattern = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    threshold_ratio = 0.65

    def clean_comment(comment):
        comment = comment.lower().strip()
        emojis = emoji.emoji_count(comment)
        text_characters = len(re.sub(r'\s', '', comment))
        if (any(char.isalnum() for char in comment)) and not hyperlink_pattern.search(comment):
            if emojis == 0 or (text_characters / (text_characters + emojis)) > threshold_ratio:
                return comment
        return None

    df['comment'] = df['comment'].apply(clean_comment)
    df.dropna(subset=['comment'], inplace=True)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue(), df

def load_to_s3(**context):
    csv_buffer, _ = context['task_instance'].xcom_pull(task_ids='extract_comments')
    try:
        s3_resource = boto3.resource(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        s3_resource.Object(BUCKET_NAME, 'comments.csv').put(Body=csv_buffer)
    except Exception as e:
        print(f"AWS access not allowed: {e}")

    return "Data loaded successfully"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'youtube_comments_extraction',
    default_args=default_args,
    description='Extract comments from a YouTube video daily',
    schedule_interval='@daily',
)

extract_comments_task = PythonOperator(
    task_id='extract_comments',
    python_callable=extract_comments,
    dag=dag,
)

load_to_s3_task = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3,
    provide_context=True,
    dag=dag,
)

extract_comments_task >> load_to_s3_task
