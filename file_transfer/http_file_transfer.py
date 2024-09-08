import requests
from dataclasses import dataclass
from typing import Final, Callable, Optional
from requests.exceptions import HTTPError, RequestException

from dataclasses import dataclass
from typing import Callable, Optional

@dataclass(frozen=True)
class ApiConfig:
    """Configuration class for API client."""
    api_url: str
    token_url: str
    timeout: int
    client_id: str
    client_secret: str
    token_interceptor: Optional[Callable[[], str]] = None

class ApiClient:
    """Client class for interacting with the API."""

    def __init__(self, config: ApiConfig):
        self.config = config
        self.token_interceptor = config.TOKEN_INTERCEPTOR or self._default_token_interceptor

    def _default_token_interceptor(self) -> str:
        """Default method for obtaining an OAuth2 token."""
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.config.CLIENT_ID,
            'client_secret': self.config.CLIENT_SECRET
        }
        try:
            response = requests.post(self.config.TOKEN_URL, data=payload)
            response.raise_for_status()  
            token_data = response.json()
            return token_data.get('access_token')
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to retrieve OAuth2 token: {e}")
        except KeyError:
            raise RuntimeError("Token response is missing 'access_token' field")

    def upload_file_to_api(self, local_file: str) -> None:
        """Uploads a file to the configured API endpoint."""
        try:
            auth_token = self.token_interceptor()
            with open(local_file, 'rb') as file:
                response = requests.post(
                    self.config.API_URL,
                    files={'file': file},
                    headers={'Authorization': f'Bearer {auth_token}'},
                    timeout=self.config.TIMEOUT
                )
            response.raise_for_status()  # Raise HTTPError for bad responses
            print("Upload Successful:", response.status_code, response.text)
        except FileNotFoundError:
            raise FileNotFoundError(f"File '{local_file}' not found")
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to upload file to API: {e}")
        except Exception as e:
            raise RuntimeError(f"An unexpected error occurred: {e}")

def get_google_token(api_config: ApiConfig) -> str:
    """Custom token retrieval logic for Google OAuth2 using configuration."""
    payload = {
        'grant_type': 'client_credentials',
        'client_id': api_config.CLIENT_ID,
        'client_secret': api_config.CLIENT_SECRET
    }
    try:
        response = requests.post(api_config.TOKEN_URL, data=payload)
        response.raise_for_status()  # Raise HTTPError for bad responses
        token_data = response.json()
        # Ensure 'access_token' is in the response data
        if 'access_token' not in token_data:
            raise ValueError("Access token not found in response")
        return token_data['access_token']
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise
    except RequestException as req_err:
        print(f"Request error occurred: {req_err}")
        raise
    except ValueError as val_err:
        print(f"Value error occurred: {val_err}")
        raise
    except Exception as err:
        print(f"An unexpected error occurred: {err}")
        raise

def no_interceptor() -> str:
    return ''

if __name__ == "__main__":
    api_config = ApiConfig(
        api_url='https://jsonplaceholder.typicode.com/posts',
        token_url='https://oauth2.googleapis.com/token',
        timeout=30,
        client_id='YOUR_CLIENT_ID',
        client_secret='YOUR_CLIENT_SECRET', 
        token_interceptor= lambda: no_interceptor()  
     #   TOKEN_INTERCEPTOR= lambda: get_google_token(api_config)  
    )
    api_client = ApiClient(api_config)
    api_client.upload_file_to_api('./data.parquet')