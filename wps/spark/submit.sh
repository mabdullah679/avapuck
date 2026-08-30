#!/usr/bin/env bash
# Submit the medallion build to the Spark cluster.
#
# Runs INSIDE the master container, so the JDK is the image's 17 and the host's
# Java 26 never enters the picture.
#
# MEMORY: this Docker has ~7.8GB shared with many unrelated containers, and an
# uncapped driver JVM will size its heap from the host and get OOM-killed. The
# driver heap is therefore capped explicitly rather than left to default.
set -euo pipefail

docker exec -e WPS_SHUFFLE_PARTITIONS="${WPS_SHUFFLE_PARTITIONS:-2}" wps-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name wps-medallion \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --driver-memory 700m \
  --conf spark.executor.memory=450m \
  --conf spark.executor.cores=1 \
  --conf spark.cores.max=1 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.driver.maxResultSize=256m \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2 \
  /opt/wps/wps/spark/medallion_spark.py
