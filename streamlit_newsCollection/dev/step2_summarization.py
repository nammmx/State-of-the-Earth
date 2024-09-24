import streamlit as st
import pandas as pd
import boto3
import openai
import requests
import json
import creds
from transformers import AutoTokenizer
from datetime import datetime

# Initialize S3 client
s3_client = boto3.client('s3', aws_access_key_id=creds.AWS_ACCESS_KEY, aws_secret_access_key=creds.AWS_SECRET_KEY)

# OpenAI API key setup
openai.api_key = creds.OPENAI_APIKEY

# Hugging Face API details for BART summarization
HF_API_URL = "https://api-inference.huggingface.co/models/facebook/bart-large-cnn"
HF_API_KEY = creds.HF_APIKEY  # Add your Hugging Face API key in creds.py
headers = {"Authorization": f"Bearer {HF_API_KEY}"}

# Load the tokenizer
tokenizer = AutoTokenizer.from_pretrained("facebook/bart-large-cnn")

# Tokenize and truncate the content to 1024 tokens
def truncate_text_with_tokenizer(text, max_length=1024):
    inputs = tokenizer(text, max_length=max_length, truncation=True, return_tensors="pt")
    truncated_text = tokenizer.decode(inputs['input_ids'][0], skip_special_tokens=True)
    return truncated_text

# Summarization using Hugging Face Inference API with tokenized input
def generate_summary_with_hf_api(text):
    # First, truncate the content using the tokenizer
    truncated_content = truncate_text_with_tokenizer(text)
    
    # Prepare the payload for the API call
    payload = {
        "inputs": truncated_content,
        "parameters": {
            "min_length": 150,
            "max_length": 300,
            "truncation": True
        }
    }
    
    # Call the Hugging Face API
    response = requests.post(HF_API_URL, headers=headers, json=payload)
    
    if response.status_code == 200:
        return response.json()[0]["summary_text"]
    else:
        st.error("Error from Hugging Face API: " + response.text)
        return None

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
def save_csv_to_s3(df):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"2_summarized_with_topics_{timestamp}.csv"
    local_csv_path = f"/tmp/{csv_filename}"
    
    # Save the DataFrame to a local CSV
    df.to_csv(local_csv_path, index=False)
    
    # Upload the new CSV to the "2_summarized_with_topics" folder in S3
    s3_key = f"2_summarized_with_topics/{csv_filename}"
    s3_client.upload_file(local_csv_path, creds.S3_BUCKET_NAME, s3_key)
    
    return local_csv_path, csv_filename

# Process the uploaded CSV (summarization and topic generation)
def process_uploaded_csv(uploaded_file):
    df = pd.read_csv(uploaded_file)
    
    # Remove rows where 'Content' is empty or NaN
    df = df[df['Content'].notna()]
    
    if df.empty:
        st.warning("No valid content found in the CSV.")
        return None
    
    # Iterate through the DataFrame and summarize articles and generate topics
    summaries = []
    topics_1 = []
    topics_2 = []
    
    for _, row in df.iterrows():
        content = row['Content']
        
        # Generate the summary using Hugging Face API
        summary = generate_summary_with_hf_api(content)
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
    return df