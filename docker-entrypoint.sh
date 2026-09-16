#!/usr/bin/env bash
# Entrypoint for the single-container Atlas image.
#
# Rewrites the Kafka-related keys in atlas-application.properties so
# that the embedded Kafka broker (a) uses the conventional 9092/2181
# port pair instead of Atlas's built-in 9027/9026 defaults, and (b)
# advertises an address that external clients can actually reach.
# Then hands off to atlas_start.py, which under the embedded-hbase-solr
# profile also starts the bundled HBase + Solr.

set -euo pipefail

CONF="${ATLAS_HOME}/conf/atlas-application.properties"

# Set a key=value pair in the properties file: replace it in place if
# it already exists (commented or uncommented), otherwise append it.
set_prop() {
  local key="$1"
  local val="$2"
  # Escape characters that are special in sed's replacement string.
  local esc_val
  esc_val=$(printf '%s' "$val" | sed -e 's/[\/&|]/\\&/g')
  if grep -Eq "^[#[:space:]]*${key}[[:space:]]*=" "$CONF"; then
    sed -i -E "s|^[#[:space:]]*${key}[[:space:]]*=.*|${key}=${esc_val}|" "$CONF"
  else
    printf '\n%s=%s\n' "$key" "$val" >> "$CONF"
  fi
}

echo "[entrypoint] configuring embedded Kafka: advertised=${KAFKA_ADVERTISED_HOST}:${KAFKA_ADVERTISED_PORT}"

# Keep the embedded Kafka's ZooKeeper on Atlas's default port 9026 —
# putting it on 2181 collides with HBase's own embedded ZooKeeper, which
# is also on 2181 (HBase 2.6.4 default) and starts first. Only the Kafka
# broker itself is moved to the conventional 9092 and given an externally
# reachable advertised address, so ordinary Kafka clients on the host can
# connect without knowing about Atlas's private port scheme.
set_prop "atlas.notification.embedded"       "true"
set_prop "atlas.kafka.zookeeper.connect"     "localhost:9026"
set_prop "atlas.kafka.zookeeper.port"        "9026"
set_prop "atlas.kafka.bootstrap.servers"     "localhost:9092"
set_prop "atlas.kafka.port"                  "9092"
set_prop "atlas.kafka.listeners"             "PLAINTEXT://0.0.0.0:9092"
set_prop "atlas.kafka.advertised.listeners"  "PLAINTEXT://${KAFKA_ADVERTISED_HOST}:${KAFKA_ADVERTISED_PORT}"
set_prop "atlas.kafka.advertised.host.name"  "${KAFKA_ADVERTISED_HOST}"
set_prop "atlas.kafka.advertised.port"       "${KAFKA_ADVERTISED_PORT}"

echo "[entrypoint] starting Atlas (this brings up HBase, Solr, Kafka, ZK, Atlas server)..."
# atlas_start.py launches HBase, Solr, and the Atlas server as background
# daemons and then exits itself — so we can't just wait on its PID (that
# would return immediately and kill PID 1). Instead, we run it, then read
# the atlas.pid file it drops and tail /dev/null against that pid so the
# container stays alive exactly as long as the Atlas JVM does.
python3 "${ATLAS_HOME}/bin/atlas_start.py"

ATLAS_PID_FILE="${ATLAS_HOME}/logs/atlas.pid"
for _ in $(seq 1 30); do
  [[ -s "$ATLAS_PID_FILE" ]] && break
  sleep 1
done
if [[ ! -s "$ATLAS_PID_FILE" ]]; then
  echo "[entrypoint] atlas.pid never appeared under ${ATLAS_HOME}/logs — Atlas failed to start."
  echo "[entrypoint] last 40 lines of application.log:"
  tail -40 "${ATLAS_HOME}/logs/application.log" 2>/dev/null || true
  exit 1
fi
ATLAS_PID=$(cat "$ATLAS_PID_FILE")
echo "[entrypoint] Atlas JVM pid=${ATLAS_PID}"

# Best-effort readiness log so the user sees a clear "we're up" line.
(
  for _ in $(seq 1 120); do
    if curl -sf -u admin:admin "http://localhost:21000/api/atlas/admin/version" >/dev/null 2>&1; then
      echo "[entrypoint] Atlas is up: http://localhost:21000  (admin/admin)"
      echo "[entrypoint] Kafka reachable at ${KAFKA_ADVERTISED_HOST}:${KAFKA_ADVERTISED_PORT}"
      exit 0
    fi
    sleep 5
  done
  echo "[entrypoint] warning: Atlas REST API did not respond within ~10 minutes; check logs under ${ATLAS_LOG_DIR}"
) &

# Forward SIGTERM/SIGINT to Atlas so `docker stop` shuts down cleanly.
trap 'echo "[entrypoint] stopping Atlas..."; python3 "${ATLAS_HOME}/bin/atlas_stop.py" || kill "${ATLAS_PID}" 2>/dev/null || true' TERM INT

# Block on the Atlas JVM. `tail --pid` is the standard trick for waiting
# on a process the current shell doesn't own — exits the moment the JVM
# does, propagating that to PID 1 so `docker stop` / crash handling work.
tail --pid="${ATLAS_PID}" -f /dev/null &
wait $!
