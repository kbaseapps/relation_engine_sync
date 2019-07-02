"""
This is the entrypoint for running the app. A parent supervisor process that
launches and monitors child processes and threads.
"""
import time

from .utils.worker_group import WorkerGroup
from .utils.config import get_config
from . import kafka_consumer

_CONFIG = get_config()


def main():
    """
    Starts all subprocesses with healthchecks
    """
    print('kafka_consumer.run', kafka_consumer.run)
    print("running...")
    print("done running...???")
    consumers = WorkerGroup(target=kafka_consumer.run, args=(), count=_CONFIG['num_consumers'])  # type: ignore
    print('consumers', consumers)
    while True:
        # Monitor processes/threads and restart any that have crashed
        consumers.health_check()
        time.sleep(5)


if __name__ == '__main__':
    main()
