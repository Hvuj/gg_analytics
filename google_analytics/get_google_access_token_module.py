from __future__ import annotations
from typing import Union, Final
from requests.adapters import HTTPAdapter, Retry, Response
from requests.exceptions import HTTPError, ConnectionError, ConnectTimeout, TooManyRedirects
import requests

session = requests.Session()

retries = Retry(total=15,
                connect=10,
                read=10,
                status=10,
                backoff_factor=1,
                status_forcelist=list(range(400, 600)),
                raise_on_status=True)

max_retries = requests.adapters.DEFAULT_RETRIES = retries
time_out_factor = requests.adapters.DEFAULT_POOL_TIMEOUT = None
adapter = HTTPAdapter(pool_connections=15,
                      pool_maxsize=15,
                      pool_block=False,
                      max_retries=max_retries)

session.mount('https://', adapter)


def get_google_access_token(client_id: str,
                            client_secret: str,
                            redirect_uri: str,
                            refresh_token: str) -> Union[str, bool]:
    """
    Fetches Google Ads api Access Token

    Parameters
    ----------
    client_id : str
        The client id of the account.
    client_secret : str
        The client secret of the account.
    redirect_uri : str
        The redirect uri that was submitted on the app or when created the code for OAuth2.0.
    refresh_token : str
        The refresh token that was received on the time when OAuth2.0 flow was completed.

    Returns
    -------
    access_token: Union[str,bool]
        On success - Access token is granted.
        On failure - None

    Raises
    ------
    HTTPError
        If url is not correct parameter.
    ConnectTimeout
        If connection is timeout.
    ConnectionError
        If there is a built-in connection error.
    """

    base_url: Final[str] = 'https://oauth2.googleapis.com/token'
    grant_type: Final[str] = 'refresh_token'
    access_type: Final[str] = 'offline'
    prompt: Final[str] = 'consent'
    url: Final[str] = f'{base_url}?' \
                      f'client_id={client_id}&' \
                      f'client_secret={client_secret}&' \
                      f'grant_type={grant_type}&' \
                      f'redirect_uri={redirect_uri}&' \
                      f'refresh_token={refresh_token}&' \
                      f'access_type={access_type}&' \
                      f'prompt={prompt}'
    payload: Final[dict] = {}
    headers: Final[dict] = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    def access_token_api_call(api_call_url: str, api_call_payload: dict, api_call_headers: dict,
                              api_call_seconds: int) -> Union[str, bool]:
        """
        Fetches Google Ads api Access Token

        Parameters
        ----------
        api_call_url : str
            The url for the current API call.
        api_call_payload : str
            The payload for the current API call.
        api_call_headers : str
            The headers for the current API call.
        api_call_seconds : int
            The seconds for the timeout window for the current API call.

        Returns
        -------
        access_token: Union[str,bool]
            On success - Access token is granted.
            On failure - None
        """
        response: Response = session.post(url=api_call_url,
                                          data=api_call_payload,
                                          headers=api_call_headers,
                                          verify=True,
                                          timeout=api_call_seconds)
        if 200 <= response.status_code < 300:
            return response.json()['access_token'] if 'access_token' in response.json() else False
        return False

    try:
        return access_token_api_call(api_call_url=url,
                                     api_call_payload=payload,
                                     api_call_headers=headers,
                                     api_call_seconds=60)
    except ConnectTimeout as connection_timeout_error:
        try:
            print(f'There was a {connection_timeout_error}. \n trying double the time request.')
            return access_token_api_call(api_call_url=url,
                                         api_call_payload=payload,
                                         api_call_headers=headers,
                                         api_call_seconds=60 * 2)
        except ConnectTimeout as second_connection_timeout_error:
            print('Tried the double amount of time and failed!')
            raise second_connection_timeout_error
    except (TooManyRedirects, HTTPError, ConnectionError) as errors:
        print(f'There is an error: {errors}')
        raise errors
