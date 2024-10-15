import json
import boto3
import pandas as pd
import spacy
import os
from datetime import datetime
from io import StringIO

# Initialize spaCy model and S3 client
nlp = spacy.load("en_core_web_sm")
s3_client = boto3.client("s3")
S3_BUCKET = "state-of-the-earth"

# S3 keys for files
FINAL_DATA_KEY = "4_final/final_data_for_flask.csv"
EXCLUSION_FILE_KEY = "wordcloud/exclusion_words.txt"
WORDCLOUD_DATA_KEY = "wordcloud/wordcloud_data_cleaned.csv"
ARCHIVE_FOLDER = "wordcloud/wordcloud_data_clean_archive"

def load_exclusion_list():
    """Load exclusion list from S3 as a set."""
    exclusion_words = set()
    try:
        exclusion_file = s3_client.get_object(Bucket=S3_BUCKET, Key=EXCLUSION_FILE_KEY)
        content = exclusion_file["Body"].read().decode("utf-8")
        exclusion_words = set(word.strip().lower() for word in content.splitlines() if word.strip())
    except Exception as e:
        print(f"Error loading exclusion list: {e}")
    return exclusion_words

def clean_data():
    """Load data, clean content with NLP, and save to S3."""
    # Load the exclusion list
    exclusion_words = load_exclusion_list()
    
    # Load data from S3
    final_data_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=FINAL_DATA_KEY)
    data = pd.read_csv(final_data_obj["Body"])

    # Define POS tags to keep
    pos_to_keep = {"NOUN", "ADJ", "PROPN"}

    # Initialize list for cleaned content
    cleaned_content = []

    # Process each row in 'content' column
    for content in data["content"].dropna().astype(str):
        doc = nlp(content)
        filtered_words = [
            token.lemma_.lower() for token in doc
            if not token.is_stop and token.is_alpha and token.pos_ in pos_to_keep
        ]
        # Apply exclusion list as the final step
        final_words = [word for word in filtered_words if word not in exclusion_words]
        cleaned_text = " ".join(final_words)
        cleaned_content.append(cleaned_text)

    # Add cleaned content to data and remove empty rows
    data["cleaned_content"] = pd.Series(cleaned_content)
    data = data[["publish_date", "source", "topic1", "topic2", "summary", "cleaned_content"]]
    data = data[data["cleaned_content"] != ""]

    # Archive current wordcloud_data_cleaned.csv if exists
    try:
        current_file = s3_client.get_object(Bucket=S3_BUCKET, Key=WORDCLOUD_DATA_KEY)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        archive_key = f"{ARCHIVE_FOLDER}/wordcloud_data_cleaned_{timestamp}.csv"
        s3_client.copy_object(
            Bucket=S3_BUCKET,
            CopySource={"Bucket": S3_BUCKET, "Key": WORDCLOUD_DATA_KEY},
            Key=archive_key
        )
        print(f"Archived existing wordcloud_data_cleaned.csv to {archive_key}")
    except s3_client.exceptions.NoSuchKey:
        print("No existing wordcloud_data_cleaned.csv to archive.")

    # Save the cleaned data to S3
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=S3_BUCKET, Key=WORDCLOUD_DATA_KEY, Body=csv_buffer.getvalue())
    print(f"Cleaned data saved to s3://{S3_BUCKET}/{WORDCLOUD_DATA_KEY}")

def lambda_handler(event, context):
    """Lambda function entry point."""
    try:
        clean_data()
        return {
            "statusCode": 200,
            "body": json.dumps("Data cleaned and saved successfully.")
        }
    except Exception as e:
        print(f"Error in Lambda function: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps("An error occurred.")
        }