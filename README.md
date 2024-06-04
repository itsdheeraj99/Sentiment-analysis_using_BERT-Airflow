# YouTube Sentiment Analysis using BERT

This project aims to perform sentiment analysis on YouTube comments using BERT (Bidirectional Encoder Representations from Transformers). It allows you to categorize the sentiment of comments into positive, negative or neutral of YouTube videos using state-of-the-art natural language processing models.


## Implementation
- Comments Retrieval: It takes YouTube video URL as an input and the comments of this video are extracted from web using GoogleAPI services.
- Sentiment Analysis: The retrieved comments are passed through to BERT model (more about training method and fine-tuning can be found in my this [repositry](https://github.com/itsdheeraj99/Sentiment_analysis_using_BERT)), which analyzes and labels them according to their category of sentiment.
- Scheduling with Airflow: The entire process, including comment retrieval, analysis, and labeling, is orchestrated and scheduled using Apache Airflow. Airflow allows for the creation of DAGs, where each node represents a task, and the dependencies between tasks are defined.

## DAGs and Workflow
### Extract DAG
This DAG contains the tasks for comments extraction from the video using Google API services, transforming it into tabular form and storing it in S3 bucket.

**Task 1:** Utilizing a Python operator, data is retrieved through the API. This includes Python libraries like requests for initiating GET request to API and obtain the required data.
**Task 2:** After extracting the data, it is strutured into a tabular format with columns of its author of the comment, Comment and whether it is public or not.
**Task 3:** Finally, the structured data is loaded to a S3 bucket. This operation is executed using Python operator and an AWS access key to access the S3 bucket.

### Sentiment Analysis DAG
This DAG contains the tasks for performing sentiment analysis on the comments and labeling them with their respective category of Sentiment and score.

**Task 1:** Utilizing an ExternalTaskSensor operator which triggers the next DAG or task in the data pipeline when an external task is completed. This task waits until the extract_dag is not completed. Once the previous DAG is successful this task triggers and initiates the Sentiment Analysis dag.
**Task 2:** This DAG collects the comments and processes them into tokens and parses into BERT model, which is called through Hugging Face transformer's pipeline API to make a GET request to the model saved on the Hugging Face hub. On passing the text of comments through the model, it returns the category of sentiment and score which is further loaded into a S3 bucket for further analysis. 

## Conclusion
By following these tasks an end to end workflow is established in AWS WMAA. The integration of Airflow guarantees the automation of analysis and generation of reports periodically.