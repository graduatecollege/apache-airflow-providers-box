#!/usr/bin/env bash

. .venv/bin/activate

export AIRFLOW_HOME=$(pwd)/airflow
export PYTHONPATH="$PYTHONPATH:$(pwd)/src"

airflow standalone
