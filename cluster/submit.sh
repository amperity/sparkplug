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

docker compose exec master \
    /opt/spark/bin/spark-submit \
    --master spark://master:7077 \
    /mnt/code/$APP_DRIVER
