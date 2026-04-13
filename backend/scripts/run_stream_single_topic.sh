#!/usr/bin/env bash
set -e

source /etc/profile.d/public-opinion.sh

VENV_PY=/opt/pipeline/.venv/bin/python
export PYSPARK_PYTHON="$VENV_PY"
export PYSPARK_DRIVER_PYTHON="$VENV_PY"
export PYTHONPATH=/opt/pipeline/spark:/opt/pipeline:$PYTHONPATH

echo "===== python check before spark-submit ====="
"$VENV_PY" - <<'PY'
import sys
import dotenv
import modelscope
import torch
print("sys.executable =", sys.executable)
print("dotenv ok")
print("modelscope ok")
print("torch ok")
PY

exec env \
  PYSPARK_PYTHON="$VENV_PY" \
  PYSPARK_DRIVER_PYTHON="$VENV_PY" \
  PYTHONPATH="/opt/pipeline/spark:/opt/pipeline:$PYTHONPATH" \
  spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  --py-files /opt/pipeline/spark/structbert_sentiment.py,/opt/pipeline/spark/alert_mailer.py \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.session.timeZone=Asia/Shanghai \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=2g \
  --conf spark.pyspark.python="$VENV_PY" \
  --conf spark.pyspark.driver.python="$VENV_PY" \
  --conf spark.executorEnv.PYSPARK_PYTHON="$VENV_PY" \
  --conf spark.executorEnv.PYTHONPATH="/opt/pipeline/spark:/opt/pipeline" \
  /opt/pipeline/spark/stream_single_topic_to_delta.py
