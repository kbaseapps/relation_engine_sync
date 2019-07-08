"""
Wait for the RE API
"""
import time
import requests
from src.utils.config import get_config

_CONFIG = get_config()


timeout = int(time.time()) + 60
while True:
    try:
        requests.get(_CONFIG['re_api_url']).raise_for_status()
        break
    except Exception as err:
        print('Waiting to connect to RE API:', err)
        if int(time.time()) > timeout:
            raise RuntimeError('Timed out waiting for RE API')
print('RE API is live!')
