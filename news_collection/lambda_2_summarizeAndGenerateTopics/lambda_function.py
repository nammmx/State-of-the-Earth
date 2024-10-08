import os
# Setting environment variables for transformers
os.environ['TRANSFORMERS_CACHE'] = '/tmp'
os.environ['HF_HOME'] = '/tmp'

import pandas as pd
import boto3
import openai
from transformers import pipeline, AutoTokenizer
from datetime import datetime
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS S3 client initialization
s3_client = boto3.client('s3')

# OpenAI API key setup
openai.api_key = os.environ['OPENAI_API_KEY']

# Summarizer setup using bart-large-cnn
def setup_summarizer():
    model_path = "/var/task/bart-large-cnn"
    summarizer = pipeline("summarization", model=model_path, tokenizer=model_path)
    return summarizer

# Tokenizer setup for bart-large-cnn
def setup_tokenizer():
    tokenizer = AutoTokenizer.from_pretrained("facebook/bart-large-cnn")
    return tokenizer

# Fetch CSV from S3 and load it into a DataFrame
def fetch_csv_from_s3(bucket_name, csv_key):
    local_csv_path = f"/tmp/{os.path.basename(csv_key)}"
    s3_client.download_file(bucket_name, csv_key, local_csv_path)
    df = pd.read_csv(local_csv_path)
    return df

# Tokenize and truncate the content before summarization
def tokenize_and_truncate(text, tokenizer, max_length=1024):
    inputs = tokenizer(text, max_length=max_length, truncation=True)
    truncated_text = tokenizer.decode(inputs['input_ids'], skip_special_tokens=True)
    return truncated_text

# Generate the summary using the bart-large-cnn model
def generate_summary(text, summarizer, tokenizer):
    # Tokenize and truncate content
    truncated_text = tokenize_and_truncate(text, tokenizer)
    # Generate the summary with the truncated content
    summary = summarizer(truncated_text, min_length=100, max_length=200, truncation=True)[0]['summary_text']
    return summary

# Generate topics using OpenAI
def generate_topics(article):
    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",  
        messages=[
            {
                "role": "system",
                "content": "You are an expert on categorizing articles based on provided topics."
            },
            {
                "role": "user",
                "content": f"""Determine the 2 topics that best fit the news article below from the following list of topics only: 
                “Agriculture & Food”, “Business & Innovation”, “Climate Change”, “Crisis & Disasters”, “Energy”, “Fossil Fuels”, “Pollution”, “Politics & Law”, “Public Health & Environment”, “Society & Culture”, “Sustainability”, “Technology & Science”, “Urban & Infrastructure”, “Water & Oceans”, “Wildlife & Conservation”.
                Topic1 is the best fitting topic, and Topic2 is the second best fitting. Return your answer in the following format: topic1-topic2
                Article: {article}"""
            }
        ]
    )
    topics = response['choices'][0]['message']['content']
    return topics.split('-')

# Save updated DataFrame with new columns to S3
def save_csv_to_s3(df, bucket_name):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"2_summarized_with_topics_{timestamp}.csv"
    local_csv_path = f"/tmp/{csv_filename}"
    
    # Save the DataFrame to a local CSV
    df.to_csv(local_csv_path, index=False)
    
    # Upload the new CSV to the "2_summarized_with_topics" folder in S3
    s3_key = f"2_summarized_with_topics/{csv_filename}"
    s3_client.upload_file(local_csv_path, bucket_name, s3_key)
    logger.info(f"Uploaded updated CSV with summaries and topics to S3: {s3_key}")

def process_csv(bucket_name, csv_key):
    # Step 1: Fetch the uploaded CSV from S3
    df = fetch_csv_from_s3(bucket_name, csv_key)

    # Step 2: Remove rows where 'Content' is empty or NaN
    df = df[df['Content'].notna()]

    if df.empty:
        logger.info("No valid content found in the CSV.")
        return

    # Step 3: Set up the summarizer (bart-large-cnn)
    summarizer = setup_summarizer()
    tokenizer = setup_tokenizer()

    # Step 4: Iterate through the DataFrame and summarize articles and generate topics
    summaries = []
    topics_1 = []
    topics_2 = []
    
    for _, row in df.iterrows():
        content = row['Content']
        
        # Generate the summary
        summary = generate_summary(content, summarizer, tokenizer)
        summaries.append(summary)
        
        # Generate the topics
        topic_1, topic_2 = generate_topics(content)
        topics_1.append(topic_1.strip())
        topics_2.append(topic_2.strip())

    # Step 5: Add the new columns to the DataFrame
    df['Summary'] = summaries
    df['Topic_1'] = topics_1
    df['Topic_2'] = topics_2

    # Step 6: Save the updated DataFrame as a new CSV and upload it to S3
    save_csv_to_s3(df, bucket_name)

# Lambda function handler
def lambda_handler(event, context):
    logger.info("Lambda function started")
    
    # Get bucket and object information from the S3 event trigger
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    csv_key = event['Records'][0]['s3']['object']['key']
    
    try:
        # Process the CSV by summarizing and generating topics
        process_csv(bucket_name, csv_key)
        logger.info(f"Successfully processed and updated CSV from {csv_key}.")
        return {"statusCode": 200, "body": "Success"}
    except Exception as e:
        logger.error(f"Error processing CSV: {e}", exc_info=True)
        return {"statusCode": 500, "body": f"Error: {str(e)}"}