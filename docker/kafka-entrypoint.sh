#!/bin/sh
# Render the broker's JAAS file and the admin client config from the
# environment, then hand over to the image's own start script.
#
# Every password here comes from .env.local through compose. Nothing is
# committed, and there is no second place a Kafka credential can live
# (docs/DECISIONS.md D12). The rendered files land in /tmp, which the
# image's non-root user can write.
set -eu

: "${KAFKA_ADMIN_PASSWORD:?KAFKA_ADMIN_PASSWORD is not set; add it to .env.local}"
: "${KAFKA_PIPELINE_PASSWORD:?KAFKA_PIPELINE_PASSWORD is not set; add it to .env.local}"
: "${KAFKA_CONSUMER_PASSWORD:?KAFKA_CONSUMER_PASSWORD is not set; add it to .env.local}"
: "${KAFKA_UI_PASSWORD:?KAFKA_UI_PASSWORD is not set; add it to .env.local}"
: "${KAFKA_WATCHDOG_PASSWORD:?KAFKA_WATCHDOG_PASSWORD is not set; add it to .env.local}"

# Broker-side JAAS for the KRaft node: the inter-broker identity plus every
# client principal the ACLs in kafka-init.sh refer to. `admin` is the only
# super user (see KAFKA_SUPER_USERS in docker-compose.yml).
cat > /tmp/kafka-jaas.conf <<EOF
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin"
  password="${KAFKA_ADMIN_PASSWORD}"
  user_admin="${KAFKA_ADMIN_PASSWORD}"
  user_pipeline="${KAFKA_PIPELINE_PASSWORD}"
  user_consumer="${KAFKA_CONSUMER_PASSWORD}"
  user_ui="${KAFKA_UI_PASSWORD}"
  user_watchdog="${KAFKA_WATCHDOG_PASSWORD}";
};
EOF

# Admin client config: used by the healthcheck and by kafka-init.sh. The one
# set of credentials that can change the cluster's authorization.
cat > /tmp/admin.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}";
EOF
chmod 600 /tmp/kafka-jaas.conf /tmp/admin.properties

export KAFKA_OPTS="-Djava.security.auth.login.config=/tmp/kafka-jaas.conf ${KAFKA_OPTS:-}"

# A one-shot init container passes its own command; the broker runs the
# image's start script.
if [ "$#" -gt 0 ]; then
  exec "$@"
fi
exec /etc/kafka/docker/run
