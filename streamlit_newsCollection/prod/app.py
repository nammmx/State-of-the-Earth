import streamlit as st
import pandas as pd
import boto3
import step1_scraping as scraper
from step2_summarization import process_uploaded_csv as process_uploaded_csv_step2, save_csv_to_s3 as save_csv_to_s3_step2
from step3_image_gen import process_uploaded_csv as process_uploaded_csv_step3, save_csv_to_s3 as save_csv_to_s3_step3
from step4_insert_redshift import generate_redshift_copy_query
import os
from io import BytesIO
from datetime import datetime

# Streamlit app
st.title("State of the Earth - News Article Collection - Manual Workflow")
st.write("")

# Step 1: Scrape News Articles
st.subheader("Step 1: Scrape News Articles")
st.markdown("[Log](https://eu-north-1.console.aws.amazon.com/cloudwatch/home?region=eu-north-1#logsV2:log-groups/log-group/$252Faws$252Flambda$252Fstate-of-the-earth-news-scraper)")

# Step 1: Button to run the scraper
if st.button("Run Scraper"):
    with st.spinner("Scraping articles..."):
        # Run the scraper to fetch and process the articles
        csv_file, df = scraper.main()

        if csv_file is not None and df is not None:
            st.success("Scraping completed!")
            st.dataframe(df)  # Display the DataFrame in Streamlit

            # Provide a download link for the CSV file
            with open(csv_file, "rb") as file:
                st.download_button(label="Download Scraped Articles CSV", data=file, file_name=os.path.basename(csv_file))
        else:
            st.warning("No articles were scraped.")

st.write("")
st.write("")

# Step 2: Summarize Articles and Generate Topics
st.subheader("Step 2: Summarize Articles and Generate Topics")
st.markdown("[Log](https://eu-north-1.console.aws.amazon.com/cloudwatch/home?region=eu-north-1#logsV2:log-groups/log-group/$252Faws$252Flambda$252Fstate-of-the-earth-summarizer-topic)")

# Step 2: Upload a CSV file for summarization and topic generation
uploaded_file_step2 = st.file_uploader("Upload a CSV for Summarization and Topic Generation", type=["csv"], key="step2")

if uploaded_file_step2:
    # Step 2: Button to run summarization and topic generation
    if st.button("Run Summarization and Topic Generation"):
        with st.spinner('Processing...'):
            # Process the uploaded CSV
            processed_df, local_csv_path, csv_filename = process_uploaded_csv_step2(uploaded_file_step2, s3_bucket_name='state-of-the-earth')
            
            if processed_df is not None:
                # Save and display the processed CSV
                st.success(f"Summarization completed and uploaded to S3 as {csv_filename}.")
                
                # Display the processed CSV
                st.dataframe(processed_df)

                # Provide download link for the CSV
                with open(local_csv_path, "rb") as file:
                    st.download_button(label="Download Processed CSV", data=file, file_name=csv_filename)
            else:
                st.warning("No valid content found in the CSV.")

st.write("")
st.write("")

# Step 3: Generate Images
st.subheader("Step 3: Generate images")
st.markdown("[Log](https://eu-north-1.console.aws.amazon.com/cloudwatch/home?region=eu-north-1#logsV2:log-groups/log-group/$252Faws$252Flambda$252Fstate-of-the-earth-image-generator)")

# Step 3: Upload a CSV file for image generation
uploaded_file_step3 = st.file_uploader("Upload a CSV for Image Generation", type=["csv"], key="step3")

if uploaded_file_step3:
    # Step 3: Button to run image generation
    if st.button("Generate Images"):
        with st.spinner('Generating images...'):
            # Process the uploaded CSV
            processed_df = process_uploaded_csv_step3(uploaded_file_step3)
            
            if processed_df is not None:
                # Save and display the processed CSV
                local_csv_path, csv_filename = save_csv_to_s3_step3(processed_df, bucket_name='state-of-the-earth')
                
                st.success(f"Image generation completed and uploaded to S3 as {csv_filename}.")
                
                # Display the processed CSV
                st.dataframe(processed_df)

                # Provide download link for the CSV
                with open(local_csv_path, "rb") as file:
                    st.download_button(label="Download Processed CSV", data=file, file_name=csv_filename)
            else:
                st.warning("No valid content found in the CSV.")

st.write("")
st.write("")

# Initialize S3 client
s3_client = boto3.client('s3', aws_access_key_id=st.secrets['AWS_ACCESS_KEY'], aws_secret_access_key=st.secrets['AWS_SECRET_KEY'])

# Step 4: Insert data into Redshift (Print the COPY query)
st.subheader("Step 4: Generate Redshift COPY Query and Upload CSV to S3")

# Upload the CSV file
uploaded_file_step4 = st.file_uploader("Upload a CSV for Redshift COPY Query", type=["csv"], key="step4")

if uploaded_file_step4:
    # Read the CSV into a pandas DataFrame
    df = pd.read_csv(uploaded_file_step4)

    # Display the uploaded CSV file in Streamlit
    st.write("### Uploaded CSV File:")
    st.dataframe(df)  # Display the DataFrame in Streamlit

    # Button to generate the Redshift COPY query and upload CSV to S3
    if st.button("Generate Redshift COPY Query and Upload to S3"):
        with st.spinner('Generating COPY query and uploading CSV...'):
            # Generate the COPY query
            copy_query = generate_redshift_copy_query(uploaded_file_step4, "state-of-the-earth")
            st.code(copy_query)  # Display the query in a code block in Streamlit

            # Upload CSV to S3
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            s3_key = f"3.1_generated_images/{uploaded_file_step4.name.split('.')[0]}.csv"
            
            # Use BytesIO to read the file content
            uploaded_file_step4.seek(0)  # Move the cursor to the start of the file
            file_data = BytesIO(uploaded_file_step4.read())  # Read the file content into memory
            
            # Upload to S3
            s3_client.upload_fileobj(file_data, "state-of-the-earth", s3_key)

            st.success(f"CSV uploaded to S3 bucket 'state-of-the-earth' in folder '3.1_generated_images' with key: {s3_key}")