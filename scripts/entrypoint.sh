#!/bin/sh
# Runs the server
set -e

export PYTHONPATH=/kb/module
python -m src.main
