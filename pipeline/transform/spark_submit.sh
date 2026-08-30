#!/usr/bin/env bash
# Submit the encrypt stage to the Spark cluster.
#
# Runs INSIDE the master container so the JVM is the image's JDK 17. The host
# carries Java 26, which Spark 4 does not support -- submitting from the host
# fails at JavaSparkContext construction. This is why the submit is a script
# and not a bare spark-submit call.
set -euo pipefail

LOGICAL_DATE="${1:?usage: spark_submit.sh YYYY-MM-DD}"

docker exec \
  -e IDP_URL="${IDP_URL:-http://idp:8443}" \
  -e CRYPTO_URL="${CRYPTO_URL:-http://crypto:8444}" \
  -e CLIENT_SECRET_SPARK_JOB="${CLIENT_SECRET_SPARK_JOB:-dev-spark-secret}" \
  -e SPARK_MASTER_URL="spark://spark-master:7077" \
  pl-spark-master \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --name pipeline-encrypt \
    --driver-memory 900m \
    --conf spark.executor.memory=768m \
    --conf spark.executor.cores=1 \
    --conf spark.cores.max=2 \
    --conf spark.sql.shuffle.partitions=2 \
    /opt/pipeline/pipeline/transform/spark_job.py "$LOGICAL_DATE"
