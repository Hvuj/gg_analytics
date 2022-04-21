import requests

from settings import CLIENT_SECRET, CLIENT_ID, REFRESH_TOKEN, REDIRECT_URI

token_headers = {
    "Content-Type": "application/x-www-form-urlencoded"
}

url = f'https://oauth2.googleapis.com/token?client_id={CLIENT_ID}' \
      f'&client_secret={CLIENT_SECRET}' \
      f'&grant_type=refresh_token' \
      f'&redirect_uri={REDIRECT_URI}' \
      f'&refresh_token={REFRESH_TOKEN}' \
      f'&access_type=offline' \
      f'&prompt=consent' \
      f'&scope=https://mail.google.com/'


def get_access_token() -> str:
    response = requests.post(url=url, headers=dict(token_headers))
    access_token = response.json()
    return access_token['access_token']