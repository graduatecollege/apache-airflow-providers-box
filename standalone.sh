#!/usr/bin/env bash

. .venv/bin/activate

export AIRFLOW_HOME=$(pwd)/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH="$PYTHONPATH:$(pwd)/src"

pdm run airflow standalone
