import os
import boto3
import psycopg2
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 client initialization
s3_client = boto3.client('s3')

# Redshift credentials (should be stored in creds.py or AWS Secrets Manager)
REDSHIFT_HOST = os.environ['REDSHIFT_HOST']
REDSHIFT_PORT = os.environ['REDSHIFT_PORT']
REDSHIFT_DBNAME = os.environ['REDSHIFT_DBNAME'] 
REDSHIFT_USER = os.environ['REDSHIFT_USER']
REDSHIFT_PASSWORD = os.environ['REDSHIFT_PASSWORD']

# EventBridge client initialization
eventbridge_client = boto3.client('events')

def copy_csv_to_redshift(bucket_name, csv_key):
    """
    Load CSV from S3 into Redshift using COPY command.
    """
    # Build the S3 file path
    s3_file_path = f"s3://{bucket_name}/{csv_key}"
    
    # Redshift COPY command
    copy_query = f"""
    COPY ingestion.news_articles(source, publish_date, title, link, content, summary, topic1, topic2, image)
    FROM '{s3_file_path}'
    IAM_ROLE '{os.environ['IAM_ROLE']}'
    CSV
    IGNOREHEADER 1
    REGION 'eu-north-1'
    DELIMITER ','
    TIMEFORMAT 'auto'
    TRUNCATECOLUMNS
    EMPTYASNULL
    BLANKSASNULL;
    """
    
    # Connect to Redshift
    conn = psycopg2.connect(
        dbname=REDSHIFT_DBNAME,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT
    )
    cur = conn.cursor()

    try:
        # Execute the COPY command
        logger.info(f"Running COPY command to load {s3_file_path} into Redshift...")
        cur.execute(copy_query)
        conn.commit()
        logger.info(f"Successfully copied {csv_key} into Redshift.")
    except Exception as e:
        logger.error(f"Error executing COPY command: {e}")
        conn.rollback()  # Roll back the transaction
        raise  # Re-raise the exception after logging
    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()

def send_event_to_eventbridge():
    """
    Send a custom event to EventBridge after successful completion of Lambda A.
    """
    try:
        response = eventbridge_client.put_events(
            Entries=[
                {
                    'Source': 'custom.lambda',
                    'DetailType': 'Lambda A Success',
                    'Detail': '{"status": "success"}',
                    'EventBusName': 'default'  # Use 'default' unless using a custom event bus
                }
            ]
        )
        logger.info(f"Event sent to EventBridge: {response}")
    except Exception as e:
        logger.error(f"Failed to send event to EventBridge: {e}")

def lambda_handler(event, context):
    """
    Lambda handler to process the uploaded CSV and insert its data into Redshift.
    """
    logger.info("Lambda function started")
    
    # S3 event information (bucket and object key)
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    csv_key = event['Records'][0]['s3']['object']['key']
    
    try:
        # Load CSV data from S3 into Redshift using COPY command
        copy_csv_to_redshift(bucket_name, csv_key)
        logger.info(f"CSV {csv_key} from {bucket_name} successfully inserted into Redshift.")
        
        # Send event to EventBridge upon successful insertion
        send_event_to_eventbridge()
        
        return {"statusCode": 200, "body": f"Success: Inserted {csv_key} into Redshift."}
    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)
        return {"statusCode": 500, "body": f"Error: {str(e)}"}