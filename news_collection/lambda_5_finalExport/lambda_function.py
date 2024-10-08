import boto3
import os
import time
import psycopg2
from io import StringIO
import csv
from datetime import datetime

# S3 and Redshift configurations
S3_BUCKET = 'state-of-the-earth'
FINAL_FOLDER = '4_final/'
ARCHIVE_FOLDER = '4_final/4_final_archive/'
FINAL_CSV_NAME = 'final_data_for_flask.csv'

# Redshift connection settings
REDSHIFT_HOST = os.environ['REDSHIFT_HOST']
REDSHIFT_PORT = os.environ['REDSHIFT_PORT']
REDSHIFT_DBNAME = os.environ['REDSHIFT_DBNAME'] 
REDSHIFT_USER = os.environ['REDSHIFT_USER']
REDSHIFT_PASSWORD = os.environ['REDSHIFT_PASSWORD']

def lambda_handler(event, context):
    # Establish Redshift connection
    conn = psycopg2.connect(
        dbname=REDSHIFT_DBNAME,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT
    )
    cursor = conn.cursor()
    
    # SQL query to fetch data from Redshift
    export_query = f"""SELECT *
                    FROM ingestion.news_articles
                    WHERE source IS NOT NULL
                    AND publish_date IS NOT NULL
                    AND title IS NOT NULL
                    AND link IS NOT NULL
                    AND content IS NOT NULL
                    AND summary IS NOT NULL
                    AND topic1 IS NOT NULL
                    AND topic2 IS NOT NULL
                    AND image IS NOT NULL
                    ORDER BY publish_date DESC;"""
    
    # Fetch data and write to CSV
    cursor.execute(export_query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer, quoting=csv.QUOTE_ALL)
    csv_writer.writerow(columns)  # Write header
    for row in data:
        csv_writer.writerow(row)  # Write each row of data
    
    cursor.close()
    conn.close()

    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # Archive the current final CSV if it exists
    try:
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        archived_file_key = f"{ARCHIVE_FOLDER}final_data_for_flask_{timestamp}.csv"
        
        # Copy the existing final CSV to the archive
        s3_client.copy_object(
            Bucket=S3_BUCKET,
            CopySource={'Bucket': S3_BUCKET, 'Key': f"{FINAL_FOLDER}{FINAL_CSV_NAME}"},
            Key=archived_file_key
        )
        
        # Delete the old final CSV after archiving
        s3_client.delete_object(Bucket=S3_BUCKET, Key=f"{FINAL_FOLDER}{FINAL_CSV_NAME}")
    except s3_client.exceptions.NoSuchKey:
        # If the final CSV doesn't exist, continue without archiving
        print("No existing final CSV file to archive.")
    
    # Upload the new CSV as the final CSV file
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=f"{FINAL_FOLDER}{FINAL_CSV_NAME}",
        Body=csv_buffer.getvalue()
    )
    
    print("Exported data and replaced final CSV successfully.")