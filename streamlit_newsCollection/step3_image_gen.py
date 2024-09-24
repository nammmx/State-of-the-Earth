import os
import pandas as pd
import boto3
from datetime import datetime
from PIL import Image
import io
import creds
import logging
import warnings
import streamlit as st
from stability_sdk import client
import stability_sdk.interfaces.gooseai.generation.generation_pb2 as generation
import cloudinary
import cloudinary.uploader

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS S3 client initialization
s3_client = boto3.client('s3', aws_access_key_id=creds.AWS_ACCESS_KEY, aws_secret_access_key=creds.AWS_SECRET_KEY)

# Stability AI and Cloudinary setup
def setup_ai_tools():
    stability_api = client.StabilityInference(
        key=creds.STABILITY_API_KEY,
        verbose=True,
        engine="stable-diffusion-xl-1024-v1-0"
    )
    return stability_api

# Cloudinary configuration
def configure_cloudinary():
    cloudinary.config(
        cloud_name=creds.CLOUDINARY_CLOUD_NAME,
        api_key=creds.CLOUDINARY_API_KEY,
        api_secret=creds.CLOUDINARY_API_SECRET
    )

# Generate an image based on the title and summary using Stability AI
def generate_image(stability_api, title, summary):
    prompt = f"Create a single realistic image for an environmental news website. The title is: {title}. The content is: {summary}."
    try:
        response = stability_api.generate(
            prompt=prompt, 
            steps=30, 
            cfg_scale=8.0, 
            width=1024, 
            height=1024, 
            style_preset="photographic"
        )
        
        for resp in response:
            for artifact in resp.artifacts:
                if artifact.finish_reason == generation.FILTER:
                    warnings.warn(
                        "Your request activated the API's safety filters and could not be processed."
                    )
                if artifact.type == generation.ARTIFACT_IMAGE:
                    img = Image.open(io.BytesIO(artifact.binary))
                    jpeg_buffer = io.BytesIO()
                    img.save(jpeg_buffer, format='JPEG')
                    jpeg_buffer.seek(0)
                    return jpeg_buffer.getvalue()  # Return the image as bytes

    except Exception as e:
        logger.error(f"Error generating image for title {title}: {e}")
        return None

# Upload image to Cloudinary and return the image URL
def upload_image(img_data, title):
    try:
        public_id = ''.join(c for c in title if c.isalnum()).lower()
        response = cloudinary.uploader.upload(
            file=img_data, 
            folder="state-of-the-earth/news_photographs/",
            public_id=public_id,
            overwrite=True
        )
        return response['url']
    except Exception as e:
        logger.error(f"Error uploading image: {e}")
        return None

# Save updated DataFrame with new columns to S3
def save_csv_to_s3(df, bucket_name):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"3_generated_images_{timestamp}.csv"
    local_csv_path = f"/tmp/{csv_filename}"
    
    # Save the DataFrame to a local CSV
    df.to_csv(local_csv_path, index=False)
    
    # Upload the new CSV to the "3_generated_images" folder in S3
    s3_key = f"3_generated_images/{csv_filename}"
    s3_client.upload_file(local_csv_path, bucket_name, s3_key)
    logger.info(f"Uploaded updated CSV with images to S3: {s3_key}")
    
    return local_csv_path, csv_filename

# Process the uploaded CSV file
def process_uploaded_csv(uploaded_file):
    df = pd.read_csv(uploaded_file)

    # Set up Stability AI and Cloudinary
    stability_api = setup_ai_tools()
    configure_cloudinary()

    # Generate images and upload them to Cloudinary
    image_urls = []
    
    for _, row in df.iterrows():
        title = row['Title']
        summary = row['Summary']
        
        # Generate the image
        img_data = generate_image(stability_api, title, summary)
        if img_data:
            # Upload the image and get the URL
            img_url = upload_image(img_data, title)
            image_urls.append(img_url)
        else:
            image_urls.append(None)

    # Add the new column 'Image_URL' to the DataFrame
    df['Image_URL'] = image_urls

    # Save the updated DataFrame as a new CSV and upload it to S3
    return df