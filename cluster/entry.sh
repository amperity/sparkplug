#!/bin/sh

COMMAND="$1"
SPARK_SCRIPT="${SPARK_HOME}/${COMMAND}"
shift || exit 2

if [[ -x $SPARK_SCRIPT ]]; then
    export SPARK_NO_DAEMONIZE=1
    export SPARK_DIST_CLASSPATH=$(/opt/hadoop/bin/hadoop classpath)
    exec "$SPARK_SCRIPT" "$@"
else
    exec "$COMMAND" "$@"
fi
