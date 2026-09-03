#!/bin/sh
# Create the pipeline's two Kafka topics, once, then exit.
#
# Topics are NOT auto-created (KAFKA_AUTO_CREATE_TOPICS_ENABLE=false): a typo
# in a topic name should fail loudly rather than silently produce a third
# topic nobody is consuming.
set -e
K=/opt/kafka/bin/kafka-topics.sh
BS=kafka:9092
T_ENC="${T_ENC:-rpos_encrypted}"
T_PUB="${T_PUB:-rpos_flat}"

for t in "$T_ENC" "$T_PUB"; do
  "$K" --bootstrap-server "$BS" --create --if-not-exists \
       --topic "$t" --partitions 3 --replication-factor 1
done

echo "topics now present:"
"$K" --bootstrap-server "$BS" --list
