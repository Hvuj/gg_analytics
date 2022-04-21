import os
from os.path import join, dirname

from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
CLIENT_ID = os.environ.get("CLIENT_ID")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")
TOKEN_URL = os.environ.get("TOKEN_URL")
REDIRECT_URI = os.environ.get("REDIRECT_URI")
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_NAME = os.environ.get("DATASET_NAME")
TABLE_NAME = os.environ.get("TABLE_NAME")
EMAIL_FROM = os.environ.get("EMAIL_FROM")
EMAIL_TO = os.environ.get("EMAIL_TO")
