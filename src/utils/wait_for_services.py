import time
import requests

from src.utils.config import get_config

_CONFIG = get_config()


def wait_for_services():
    """Wait for dependency services such as the RE API."""
    timeout = int(time.time()) + 60
    while True:
        try:
            requests.get(_CONFIG['re_api_url'] + '/').raise_for_status()
            break
        except Exception as err:
            print('Service not yet online', err)
            if int(time.time()) >= timeout:
                raise RuntimeError("Timed out waiting for other services to come online.")
            time.sleep(3)
    print('Services started!')


if __name__ == '__main__':
    wait_for_services()
