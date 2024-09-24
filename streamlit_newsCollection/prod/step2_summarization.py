import os
import pandas as pd
import boto3
import openai
import requests
from transformers import AutoTokenizer, pipeline
from datetime import datetime

# Initialize S3 client
s3_client = boto3.client('s3', aws_access_key_id=os.getenv('AWS_ACCESS_KEY'), aws_secret_access_key=os.getenv('AWS_SECRET_KEY'))

# OpenAI API key setup
openai.api_key = os.getenv('OPENAI_APIKEY')

# Load the tokenizer and setup summarizer
def setup_summarizer():
    model_path = "facebook/bart-large-cnn"
    summarizer = pipeline("summarization", model=model_path, tokenizer=model_path)
    return summarizer

tokenizer = AutoTokenizer.from_pretrained("facebook/bart-large-cnn")

# Tokenize and truncate the content to 1024 tokens
def tokenize_and_truncate(text, max_length=1024):
    inputs = tokenizer(text, max_length=max_length, truncation=True, return_tensors="pt")
    truncated_text = tokenizer.decode(inputs['input_ids'][0], skip_special_tokens=True)
    return truncated_text

# Generate the summary using the bart-large-cnn model
def generate_summary(content, summarizer):
    truncated_content = tokenize_and_truncate(content)
    summary = summarizer(truncated_content, min_length=100, max_length=200, truncation=True)[0]['summary_text']
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
def save_csv_to_s3(df, s3_bucket_name):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"2_summarized_with_topics_{timestamp}.csv"
    local_csv_path = f"/tmp/{csv_filename}"
    
    # Save the DataFrame to a local CSV
    df.to_csv(local_csv_path, index=False)
    
    # Upload the new CSV to the "2_summarized_with_topics" folder in S3
    s3_key = f"2_summarized_with_topics/{csv_filename}"
    s3_client.upload_file(local_csv_path, s3_bucket_name, s3_key)
    
    return local_csv_path, csv_filename

# Process the uploaded CSV (summarization and topic generation)
def process_uploaded_csv(uploaded_file, s3_bucket_name):
    df = pd.read_csv(uploaded_file)
    
    # Remove rows where 'Content' is empty or NaN
    df = df[df['Content'].notna()]
    
    if df.empty:
        return None
    
    # Set up summarizer
    summarizer = setup_summarizer()

    # Iterate through the DataFrame and summarize articles and generate topics
    summaries = []
    topics_1 = []
    topics_2 = []
    
    for _, row in df.iterrows():
        content = row['Content']
        
        # Generate the summary using BART model
        summary = generate_summary(content, summarizer)
        summaries.append(summary)
        
        # Generate the topics using OpenAI API
        topic_1, topic_2 = generate_topics(content)
        topics_1.append(topic_1.strip())
        topics_2.append(topic_2.strip())

    # Add the new columns to the DataFrame
    df['Summary'] = summaries
    df['Topic_1'] = topics_1
    df['Topic_2'] = topics_2

    # Save the updated DataFrame as a new CSV and upload it to S3
    local_csv_path, csv_filename = save_csv_to_s3(df, s3_bucket_name)

    return df, local_csv_path, csv_filename