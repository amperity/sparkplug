#!/bin/bash

APP_JAR="$1"

if [[ -z $APP_JAR ]]; then
    echo "No application jar file provided!" >&2
    exit 1
fi

if [[ ! -f jars/$APP_JAR ]]; then
    echo "Couldn't find jars/$APP_JAR - did you copy it in place?" >&2
    exit 2
fi

docker-compose exec master \
    bin/spark-submit \
    --master spark://master:7077 \
    /mnt/jars/$APP_JAR
