"""
This is the entrypoint for running the app. A parent supervisor process that
launches and monitors child processes and threads.
"""
import time

from src.utils.worker_group import WorkerGroup
from src.utils.config import get_config
from src.utils.wait_for_services import wait_for_services
from src.utils.logger import log
from src import kafka_consumer

_CONFIG = get_config()


def main():
    """
    Starts all subprocesses with ongoing healthchecks.
    Number of subprocesses can be configured with the env var 'KBASE_SECURE_CONFIG_PARAM_NUM_CONSUMERS'
    """
    wait_for_services()
    consumers = WorkerGroup(target=kafka_consumer.run, args=(), count=_CONFIG['num_consumers'])  # type: ignore
    log('INFO', f'started consumers: {consumers}')
    while True:
        # Monitor processes/threads and restart any that have crashed
        consumers.health_check()
        log('INFO', f'consumers: {consumers}')
        time.sleep(5)


if __name__ == '__main__':
    main()
