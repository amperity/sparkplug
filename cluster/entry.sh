#!/bin/bash

export SPARK_NO_DAEMONIZE=1
export SPARK_DIST_CLASSPATH=$(/opt/hadoop/bin/hadoop classpath)
exec "$@"
