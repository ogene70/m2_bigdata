#!/usr/bin/env bash

# Standalone Spark — these values can be overridden by the
# SPARK_WORKER_* environment variables passed via docker-compose.
export SPARK_MASTER_HOST=spark-master
export SPARK_WORKER_CORES="${SPARK_WORKER_CORES:-2}"
export SPARK_WORKER_MEMORY="${SPARK_WORKER_MEMORY:-2g}"
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"