import streamlit as st
import pandas as pd
import step1_scraping as scraper
from step2_summarization import process_uploaded_csv, save_csv_to_s3
from step3_image_gen import process_uploaded_csv, save_csv_to_s3

# Streamlit app
st.title("State of the Earth - News Article Collection - Manual Workflow")
st.write("")

st.subheader("Step 1: Scrape News Articles")
# Step 1: Button to run the scraper
if st.button("Run Scraper"):
    with st.spinner("Scraping articles..."):
        # Run the scraper to fetch and process the articles
        csv_file, df = scraper.main()
        
        if csv_file and df is not None:
            st.success("Scraping completed!")
            
            # Display the scraped articles as a dataframe in the app
            st.dataframe(df)

            # Provide a download link for the CSV file
            with open(csv_file, "rb") as file:
                st.download_button(label="Download Scraped Articles CSV", data=file, file_name=csv_file)
        else:
            st.warning("No articles were scraped.")
st.write("")
st.write("")
st.subheader("Step 2: Summarize Articles and Generate Topcis")
# Step 2: Upload a CSV file for summarization and topic generation
uploaded_file = st.file_uploader("Upload a CSV for Summarization and Topic Generation", type=["csv"])

if uploaded_file:
    if st.button("Run Summarization and Topic Generation"):
        with st.spinner('Processing...'):
            processed_df = process_uploaded_csv(uploaded_file)
            
            if processed_df is not None:
                # Save and display the processed CSV
                local_csv_path, csv_filename = save_csv_to_s3(processed_df)
                
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
st.subheader("Step 3: Generate images")
# Step 3: Upload a CSV file for image generation
uploaded_file = st.file_uploader("Upload a CSV for Image Generation", type=["csv"])

if uploaded_file:
    if st.button("Generate Images"):
        with st.spinner('Generating images...'):
            processed_df = process_uploaded_csv(uploaded_file)
            
            if processed_df is not None:
                # Save and display the processed CSV
                local_csv_path, csv_filename = save_csv_to_s3(processed_df, bucket_name='state-of-the-earth')
                
                st.success(f"Image generation completed and uploaded to S3 as {csv_filename}.")
                
                # Display the processed CSV
                st.dataframe(processed_df)

                # Provide download link for the CSV
                with open(local_csv_path, "rb") as file:
                    st.download_button(label="Download Processed CSV", data=file, file_name=csv_filename)
            else:
                st.warning("No valid content found in the CSV.")
