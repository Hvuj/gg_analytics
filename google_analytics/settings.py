from __future__ import annotations
from pathlib import Path
from typing import Final
from dotenv import load_dotenv
from google.cloud import secretmanager
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import os
import google_crc32c
import json
import base64

try:
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    # Load environment variables from the '.env' file
    env_path = Path(__file__).parent / '.env'
    load_dotenv(dotenv_path=env_path, override=True)
except FileNotFoundError:
    print("Could not find .env file")
    raise

PROJECT_ID: Final[str] = os.environ.get("PROJECT_ID")
SECRET_ID: Final[str] = os.environ.get("SECRET_ID")
name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"

try:
    response = client.access_secret_version(request={"name": name})
    # Verify payload checksum.
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        print("Data corruption detected.")
        print(response)
    payload = json.loads(str(response.payload.data.decode('utf-8')).replace('\'', '\"'))
except Exception as e:
    raise f"An error occurred while accessing the secret: {e}" from e

# Assign the environment variables to constants
GOOGLE_REFRESH_TOKEN = payload['GOOGLE_REFRESH_TOKEN']
GOOGLE_REDIRECT_URI = payload['GOOGLE_REDIRECT_URI']
GOOGLE_CLIENT_ID = payload['GOOGLE_CLIENT_ID']
GOOGLE_CLIENT_SECRET = payload['GOOGLE_CLIENT_SECRET']

if None in (GOOGLE_REFRESH_TOKEN, GOOGLE_REDIRECT_URI, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET):
    raise ValueError(
        "Missing environment variables."
        "Make sure to set:"
        "GOOGLE_REFRESH_TOKEN, GOOGLE_REDIRECT_URI, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET.")

# Encryption
# Generate a password
password = os.urandom(32)
# Generate a salt
salt = os.urandom(16)

try:
    # Derive the key from the password and salt
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
        backend=default_backend()
    )
    key = base64.urlsafe_b64encode(kdf.derive(password))
    cipher_suite = Fernet(key)
    ciphered_refresh_token_text = cipher_suite.encrypt(GOOGLE_REFRESH_TOKEN.encode())
    ciphered_redirect_uri_text = cipher_suite.encrypt(GOOGLE_REDIRECT_URI.encode())
    ciphered_client_id_text = cipher_suite.encrypt(GOOGLE_CLIENT_ID.encode())
    ciphered_client_secret_text = cipher_suite.encrypt(GOOGLE_CLIENT_SECRET.encode())
    # save the ciphered_text to the disk or any other storage, or you can store it in environment variable again
    os.environ["GOOGLE_REFRESH_TOKEN_CIPHERED"] = ciphered_refresh_token_text.decode()
    os.environ["GOOGLE_REDIRECT_URI_CIPHERED"] = ciphered_redirect_uri_text.decode()
    os.environ["GOOGLE_CLIENT_ID_CIPHERED"] = ciphered_client_id_text.decode()
    os.environ["GOOGLE_CLIENT_SECRET_CIPHERED"] = ciphered_client_secret_text.decode()
except Exception as e:
    raise f"Error occurred while encrypting: {e}" from e
