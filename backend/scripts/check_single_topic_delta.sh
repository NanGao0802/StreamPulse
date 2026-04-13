#!/usr/bin/env bash
set -e

source /etc/profile.d/public-opinion.sh
source /opt/pipeline/.venv/bin/activate

export PYSPARK_PYTHON=/opt/pipeline/.venv/bin/python
export PYSPARK_DRIVER_PYTHON=/opt/pipeline/.venv/bin/python

spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/pipeline/tests/check_single_topic_delta.py
