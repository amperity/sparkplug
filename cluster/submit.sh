#!/bin/bash

APP_DRIVER="$1"

if [[ -z $APP_DRIVER ]]; then
    echo "No application driver code provided!" >&2
    exit 1
fi

if [[ ! -f code/$APP_DRIVER ]]; then
    echo "Couldn't find code/$APP_DRIVER - did you copy it in place?" >&2
    exit 2
fi

shift 
docker compose exec \
    --env PYSPARK_DRIVER_PYTHON=python3 \
    --env PYSPARK_PYTHON=./venv/bin/python3 \
    --env VAULT_TOKEN="$(cat ~/.vault-token)" \
    master \
    /opt/spark/bin/spark-submit \
    --master spark://master:7077 \
    "$@" \
    /mnt/code/$APP_DRIVER
