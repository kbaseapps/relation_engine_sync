#!/bin/sh
# set -e

flake8 /kb/module/src
mypy --ignore-missing-imports /kb/module/src
bandit -r /kb/module/src
# Run the app and fork
sh /kb/module/scripts/entrypoint.sh &
# Wait for the RE API to be healthy
python /kb/module/scripts/wait_for_api.py
# Run the tests
python -m unittest discover /kb/module/src/test/
