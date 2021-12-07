import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())
host=os.environ.get('MYSQL_HOST')
user=os.environ.get('MYSQL_USERNAME')
password = os.environ.get('MYSQL_PASSWORD')
gcp_api_key=os.environ.get('GCP_API_KEY')
