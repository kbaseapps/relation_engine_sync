#!/bin/sh
# set -e

flake8 /kb/module/src
mypy --ignore-missing-imports /kb/module/src
bandit -r /kb/module/src
# Run the app and fork
sh /kb/module/scripts/entrypoint.sh &

# Wait for RE API to start
python -m src.utils.wait_for_services &&
# Run the tests
python -m unittest discover /kb/module/src/test/
