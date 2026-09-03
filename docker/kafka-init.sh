#!/bin/sh
# Create the pipeline's two Kafka topics and grant least-privilege ACLs.
#
# Runs once, then exits. Topics are NOT auto-created
# (KAFKA_AUTO_CREATE_TOPICS_ENABLE=false) and the broker denies any principal
# with no matching ACL, so both halves of this script are load-bearing: without
# it the pipeline authenticates successfully and is then refused.
set -e
K=/opt/kafka/bin/kafka-topics.sh
A=/opt/kafka/bin/kafka-acls.sh
BS=kafka:9092
CFG=/etc/kafka/admin.properties
T_ENC="${T_ENC:-rpos_encrypted}"
T_PUB="${T_PUB:-rpos_flat}"

for t in "$T_ENC" "$T_PUB"; do
  "$K" --bootstrap-server "$BS" --command-config "$CFG" \
       --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 1
done

# The pipeline WRITES and never reads: a compromised publisher should not be
# able to drain the topics it feeds.
for t in "$T_ENC" "$T_PUB"; do
  "$A" --bootstrap-server "$BS" --command-config "$CFG" --add \
       --allow-principal User:pipeline \
       --operation Write --operation Describe --operation Create \
       --topic "$t"
done

# The consumer READS, and only the flat topic. Reading rpos_encrypted requires
# a principal that is deliberately not provisioned here -- ciphertext is still
# ciphertext, but there is no reason to hand it out by default.
"$A" --bootstrap-server "$BS" --command-config "$CFG" --add \
     --allow-principal User:consumer \
     --operation Read --operation Describe \
     --topic "$T_PUB"
"$A" --bootstrap-server "$BS" --command-config "$CFG" --add \
     --allow-principal User:consumer \
     --operation Read --group '*'

echo "topics:"
"$K" --bootstrap-server "$BS" --command-config "$CFG" --list
echo "acls:"
"$A" --bootstrap-server "$BS" --command-config "$CFG" --list
