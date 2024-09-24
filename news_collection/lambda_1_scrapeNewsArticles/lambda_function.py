import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
import urllib.parse
from datetime import datetime
import pytz
import boto3
import creds
import logging
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# List of RSS feeds with their source names and URLs
RSS_FEEDS = [
    {'name': 'The Guardian', 'url': 'https://www.theguardian.com/us/environment/rss'},
    {'name': 'BBC News', 'url': 'https://feeds.bbci.co.uk/news/science_and_environment/rss.xml'},
    {'name': 'Grist', 'url': 'https://grist.org/feed/'},
    {'name': 'Earth911', 'url': 'https://earth911.com/feed/'},
    {'name': 'Columbia Climate School', 'url': 'https://news.climate.columbia.edu/feed/'},
    {'name': 'The Independent', 'url': 'https://www.independent.co.uk/climate-change/news/rss'},
    {'name': 'Yale Environment 360', 'url': 'https://e360.yale.edu/feed.xml'},
    {'name': 'Greenpeace', 'url': 'https://www.greenpeace.org/canada/en/feed/'}
]

# AWS S3 Bucket information
S3_BUCKET_NAME = "state-of-the-earth"
S3_SCRAPED_URLS_FILE = "scraped_urls.txt"
# CSV filename will be dynamically generated
CSV_FILE = None

# Headers to mimic a browser request (helps with some websites)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# S3 client initialization
s3_client = boto3.client(
    's3',
    aws_access_key_id=creds.AWS_ACCESS_KEY,
    aws_secret_access_key=creds.AWS_SECRET_KEY
)

def load_scraped_urls():
    """
    Load previously scraped URLs from S3 to avoid re-scraping the same articles.
    Returns a set of URLs. The file is located in the '1_raw' subfolder.
    """
    try:
        # Get the object from S3 with the "1_raw" subfolder path
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=f"1_raw/{S3_SCRAPED_URLS_FILE}")
        scraped_urls = response['Body'].read().decode('utf-8').splitlines()
        return set(scraped_urls)
    except s3_client.exceptions.NoSuchKey:
        return set()  # If the file doesn't exist, return an empty set

def save_scraped_url(url):
    """
    Append a newly scraped URL to the S3 file to avoid duplicate scraping in the future.
    The file is located in the '1_raw' subfolder.
    """
    # Load existing scraped URLs from S3
    scraped_urls = load_scraped_urls()
    # Add the new URL
    scraped_urls.add(url)

    # Convert back to string and upload to S3 (in the "1_raw" subfolder)
    scraped_urls_data = "\n".join(scraped_urls)
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=f"1_raw/{S3_SCRAPED_URLS_FILE}", Body=scraped_urls_data)

def fetch_content(url):
    """
    Fetch the HTML content from a given URL using requests.
    Returns the content if successful, otherwise returns None and prints an error.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()  # Check for HTTP errors
        return response.content  # Return HTML content
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return None  # Return None if there's an error

def parse_paragraphs(soup, parent_tag, child_tag='p', child_class=None, child_id=None, data_component=None):
    """
    Extract paragraphs from all matching parent HTML tags (e.g., 'div', 'article').
    This function collects all <p> tags within each matching parent and returns them as a single string.
    Only immediate child <p> tags are included (ignoring nested <p> tags).
    Optionally, it can filter by class, id, or data-component attribute.
    """
    # Find all parent tags based on the filters provided
    if child_id:
        parents = soup.find_all(parent_tag, id=child_id)
    elif child_class:
        parents = soup.find_all(parent_tag, class_=child_class)
    elif data_component:
        parents = soup.find_all(parent_tag, attrs={'data-component': data_component})
    else:
        parents = soup.find_all(parent_tag)

    if not parents:
        return ""  # Return an empty string if no parent tags are found

    # Collect paragraphs from all matching parent tags
    all_paragraphs = []
    for parent in parents:
        # Find all child <p> tags that are immediate children of the parent tag
        paragraphs = parent.find_all(child_tag, recursive=False)
        # Add the text content of the paragraphs to the list
        all_paragraphs.extend(p.get_text(" ", strip=True) for p in paragraphs)

    # Join the text content of all paragraphs and return as a single string
    return " ".join(all_paragraphs)

# Individual content parsers for each website (based on domain)

def parse_guardian(url):
    """
    Extract content from The Guardian article page.
    """
    content = fetch_content(url)
    if not content: return ""
    soup = BeautifulSoup(content, 'html.parser')
    # Find the main content div by id
    main_content = soup.find('div', id='maincontent')
    if not main_content:
        return ""
    return parse_paragraphs(main_content, 'div', 'p')

def parse_bbc(url):
    """
    Extract content from BBC News article page.
    """
    content = fetch_content(url)
    if not content: return ""
    
    soup = BeautifulSoup(content, 'html.parser')
    main_content = soup.find('article')
    if not main_content:
        return ""
    return parse_paragraphs(soup, 'div', data_component='text-block')

def parse_grist(url):
    """
    Extract content from Grist article page.
    """
    content = fetch_content(url)
    if not content: return ""
    
    soup = BeautifulSoup(content, 'html.parser')
    return parse_paragraphs(soup, 'div', child_class='article-body')

def parse_earth911(url):
    """
    Extract content from Earth911 article page.
    """
    content = fetch_content(url)
    if not content: return ""
    
    soup = BeautifulSoup(content, 'html.parser')
    return parse_paragraphs(soup, 'article')

def parse_columbia_climate(url):
    """
    Extract content from Columbia Climate School article page.
    """
    content = fetch_content(url)
    if not content: return ""
    soup = BeautifulSoup(content, 'html.parser')
    main_content = soup.find('main')
    if not main_content:
        return ""
    
    return parse_paragraphs(main_content, 'div', 'p', child_class='entry-content')

def parse_independent(url):
    """
    Extract content from The Independent article page.
    """
    content = fetch_content(url)
    if not content: return ""
    
    soup = BeautifulSoup(content, 'html.parser')
    return parse_paragraphs(soup, 'div', child_id='main')

def parse_yale_environment(url):
    """
    Extract content from Yale Environment 360 article page.
    """
    content = fetch_content(url)
    if not content: return ""
    
    soup = BeautifulSoup(content, 'html.parser')
    return parse_paragraphs(soup, 'section', 'div', child_class='article__body')

def parse_greenpeace(url):
    """
    Extract content from Greenpeace article page.
    """
    content = fetch_content(url)
    if not content: return ""
    soup = BeautifulSoup(content, 'html.parser')
    main_content = soup.find('div', id='content')
    if not main_content:
        return ""
    return parse_paragraphs(main_content, 'article', 'p')

# Mapping domains to specific parsers
def get_content_parser(domain):
    """
    Return the appropriate parser function for the given domain.
    """
    parsers = {
        'www.theguardian.com': parse_guardian,
        'www.bbc.com': parse_bbc,
        'grist.org': parse_grist,
        'earth911.com': parse_earth911,
        'news.climate.columbia.edu': parse_columbia_climate,
        'www.independent.co.uk': parse_independent,
        'e360.yale.edu': parse_yale_environment,
        'www.greenpeace.org': parse_greenpeace
    }
    return parsers.get(domain, None)

def convert_to_berlin_time(published_str):
    """
    Converts a published date string to Berlin time, stripping off timezone info.
    Returns the formatted date string in Berlin time.
    """
    try:
        # Replace "GMT" with "+0000" for compatibility with strptime
        if 'GMT' in published_str:
            published_str = published_str.replace('GMT', '+0000')
        
        # Parse the datetime string into a naive datetime object
        published_dt = datetime.strptime(published_str, '%a, %d %b %Y %H:%M:%S %z')

        # Define the timezones
        berlin = pytz.timezone('Europe/Berlin')

        # Convert to Berlin time
        published_berlin = published_dt.astimezone(berlin)

        # Format to exclude the timezone information
        return published_berlin.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error converting date: {e}")

        # Fallback: Return the string without the timezone information
        try:
            # Try to format the string by splitting and removing timezone part
            return " ".join(published_str.split()[:-1])
        except:
            return published_str  # If formatting fails, return the original string

# Parsing the RSS feed and extracting articles
def parse_feed(feed_name, feed_url, scraped_urls, max_articles=10):
    """
    Parse an RSS feed and extract articles, limiting the result to a maximum number of articles per invocation.
    """
    articles = []
    response = fetch_content(feed_url)
    if not response:
        return articles

    soup = BeautifulSoup(response, 'xml')
    count = 0  # Counter to ensure only max_articles are processed

    for item in soup.find_all('item'):
        if count >= max_articles:
            break

        link = item.find('link').text.strip()
        if link in scraped_urls:
            continue  # Skip already scraped articles

        title = item.find('title').text.strip() if item.find('title') else ''
        published = item.find('pubDate').text.strip() if item.find('pubDate') else ''
        published = convert_to_berlin_time(published)

        domain = urllib.parse.urlparse(link).netloc
        parser = get_content_parser(domain)
        content = parser(link) if parser else "Content parsing not supported."

        articles.append({'Source': feed_name, 'Published': published, 'Title': title, 'Link': link, 'Content': content})
        save_scraped_url(link)  # Save the URL after parsing
        count += 1  # Increment the counter to limit the number of articles

    return articles

def upload_to_s3(CSV_FILE):
    """
    Upload the CSV file to the specified S3 bucket within the "1_raw" subfolder.
    """
    if CSV_FILE is None:
        raise ValueError("CSV_FILE is None, cannot upload to S3")
    
    # Modify the filename to include "1_raw" subfolder and timestamp
    file_name = os.path.basename(CSV_FILE)  # Extract the filename from the full path
    s3_key = f"1_raw/{file_name}"  # Upload to the "1_raw" subfolder in the bucket
    
    try:
        s3_client.upload_file(CSV_FILE, S3_BUCKET_NAME, s3_key)
        print(f"Uploaded {file_name} to S3 bucket {S3_BUCKET_NAME} in folder '1_raw'.")
    except Exception as e:
        print(f"Error uploading {file_name} to S3: {e}")

def main():
    scraped_urls = load_scraped_urls()  # Load already scraped URLs
    all_articles = []

    # Loop through each RSS feed and scrape new articles, limited to 10 articles per feed
    for feed in RSS_FEEDS:
        print(f"Processing feed from {feed['name']}...")
        articles = parse_feed(feed['name'], feed['url'], scraped_urls, max_articles=10)
        all_articles.extend(articles)

    # Generate a timestamped filename for the CSV in the format "1_raw_*timestamp*.csv"
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    CSV_FILE = f"/tmp/1_raw_{timestamp}.csv"  # Use /tmp directory

    # If new articles are found, save them to a CSV file
    if all_articles:
        df = pd.DataFrame(all_articles)
        df.to_csv(CSV_FILE, index=False, encoding='utf-8')
        print(f"Saved {len(all_articles)} new articles to {CSV_FILE}.")

        # Upload the CSV file to the S3 subfolder "1_raw"
        upload_to_s3(CSV_FILE)

        # Delete the file from /tmp after uploading
        try:
            os.remove(CSV_FILE)
            print(f"Deleted temporary file {CSV_FILE} from /tmp.")
        except Exception as e:
            print(f"Error deleting file {CSV_FILE}: {e}")
    else:
        print("No new articles to save.")

def lambda_handler(event, context):
    logger.info("Lambda function started")
    
    try:
        main()  # Call the main processing function
        logger.info("Lambda function completed successfully")
        return {"statusCode": 200, "body": "Success"}
    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)
        return {"statusCode": 500, "body": f"Error: {str(e)}"}