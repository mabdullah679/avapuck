#!/bin/sh
# Create the pipeline's topics and grant least-privilege ACLs. Runs once.
#
# Topics are NOT auto-created (KAFKA_AUTO_CREATE_TOPICS_ENABLE=false) and the
# broker denies any principal with no matching ACL, so both halves of this
# script are load-bearing: without it the pipeline authenticates successfully
# and is then refused. Idempotent: --if-not-exists on topics, and re-adding an
# ACL is a no-op.
#
# Principals and what each may do (docs/DECISIONS.md D6, D9):
#   pipeline   WRITE the two data topics and the control topic. Reads nothing:
#              a compromised publisher cannot drain what it feeds.
#   consumer   READ the flat topic and the control topic. Never the ciphertext.
#   ui         DESCRIBE everything (topics, groups, offsets, lag); READ the
#              flat and control topics only. Read-only by ACL, not by config.
#   watchdog   READ the control topic only.
set -eu
K=/opt/kafka/bin/kafka-topics.sh
A=/opt/kafka/bin/kafka-acls.sh
BS="${KAFKA_BOOTSTRAP:-kafka:9092}"
CFG=/tmp/admin.properties
T_ENC="${KAFKA_TOPIC_ENCRYPTED:?}"
T_PUB="${KAFKA_TOPIC_PUBLIC:?}"
T_CTL="${KAFKA_TOPIC_CONTROL:?}"

# Rendered by kafka-entrypoint.sh in the broker; render our own copy here.
: "${KAFKA_ADMIN_PASSWORD:?KAFKA_ADMIN_PASSWORD is not set; add it to .env.local}"
cat > "$CFG" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}";
EOF

for t in "$T_ENC" "$T_PUB"; do
  "$K" --bootstrap-server "$BS" --command-config "$CFG" \
       --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 1
done
# The control topic: one partition, so verdicts for every dataset are totally
# ordered, and a long retention so a consumer or the watchdog can replay
# history after a restart. Six messages a day; size is not a concern.
"$K" --bootstrap-server "$BS" --command-config "$CFG" \
     --create --if-not-exists --topic "$T_CTL" --partitions 1 --replication-factor 1 \
     --config retention.ms=2592000000

acl() { "$A" --bootstrap-server "$BS" --command-config "$CFG" --add "$@" >/dev/null; }

# pipeline: write everything, read nothing.
for t in "$T_ENC" "$T_PUB" "$T_CTL"; do
  acl --allow-principal User:pipeline --operation Write --operation Describe --operation Create --topic "$t"
done
# Idempotent producer needs IdempotentWrite on the cluster.
acl --allow-principal User:pipeline --operation IdempotentWrite --cluster

# consumer: flat + control, any group.
for t in "$T_PUB" "$T_CTL"; do
  acl --allow-principal User:consumer --operation Read --operation Describe --topic "$t"
done
acl --allow-principal User:consumer --operation Read --group '*'

# ui: see everything, read only what the consumer may.
acl --allow-principal User:ui --operation Describe --operation DescribeConfigs --topic '*'
acl --allow-principal User:ui --operation Describe --operation DescribeConfigs --cluster
acl --allow-principal User:ui --operation Describe --group '*'
for t in "$T_PUB" "$T_CTL"; do
  acl --allow-principal User:ui --operation Read --topic "$t"
done
acl --allow-principal User:ui --operation Read --group '*'

# watchdog: the control topic, nothing else.
acl --allow-principal User:watchdog --operation Read --operation Describe --topic "$T_CTL"
acl --allow-principal User:watchdog --operation Read --group '*'

echo "topics:"
"$K" --bootstrap-server "$BS" --command-config "$CFG" --list
echo "acls:"
"$A" --bootstrap-server "$BS" --command-config "$CFG" --list
