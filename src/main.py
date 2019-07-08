"""
This is the entrypoint for running the app. A parent supervisor process that
launches and monitors child processes and threads.
"""
import time
import requests

from .utils.worker_group import WorkerGroup
from .utils.config import get_config
from . import kafka_consumer

_CONFIG = get_config()


def main():
    """
    Starts all subprocesses with ongoing healthchecks.
    Number of subprocesses can be configured with the env var 'KBASE_SECURE_CONFIG_PARAM_NUM_CONSUMERS'
    """
    _wait_for_services()
    consumers = WorkerGroup(target=kafka_consumer.run, args=(), count=_CONFIG['num_consumers'])  # type: ignore
    while True:
        # Monitor processes/threads and restart any that have crashed
        consumers.health_check()
        time.sleep(5)


def _wait_for_services():
    """Wait for dependency services such as the RE API."""
    timeout = int(time.time()) + 60
    while True:
        try:
            requests.get(_CONFIG['re_api_url']).raise_for_status()
            break
        except Exception as err:
            print('Service not yet online', err)
            if int(time.time()) >= timeout:
                raise RuntimeError("Timed out waiting for other services to come online.")
            time.sleep(3)
    print('Services started!')


if __name__ == '__main__':
    main()
