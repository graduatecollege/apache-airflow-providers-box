#!/usr/bin/env bash

. .venv/bin/activate

export AIRFLOW_HOME=$(pwd)/airflow

airflow standalone
