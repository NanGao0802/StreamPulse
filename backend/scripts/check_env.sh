#!/usr/bin/env bash
set -e

source /etc/profile.d/public-opinion.sh

echo "================ JAVA ================"
java -version

echo
echo "================ PYTHON ================"
python3 --version

echo
echo "================ PIP ================"
python3 -m pip --version

echo
echo "================ SPARK-SUBMIT ================"
spark-submit --version

echo
echo "================ IMPORTANT DIRS ================"
ls -ld /opt/module/spark
ls -ld /opt/pipeline
ls -ld /data/public-opinion
