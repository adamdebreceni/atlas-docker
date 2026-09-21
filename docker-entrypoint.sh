#!/usr/bin/env bash
# Entrypoint for the single-container Atlas image.
#
# Orchestrates HBase → Solr → Atlas startup with an explicit readiness
# gate between every stage. Upstream's atlas_start.py starts all three
# in one shot, swallows their exit codes, then hands off to a 5-minute
# TCP wait on port 21000 — if Solr didn't actually bind, the Atlas JVM
# retries JanusGraph→Solr init 3 times over ~2 minutes and dies, and
# the whole boot fails opaquely. Rather than trust that flow, we
# invoke atlas_start.py three times with different MANAGE_LOCAL_*
# flags so each stage runs on its own and can be verified before the
# next one starts.
#
# Also rewrites the Kafka-related keys in atlas-application.properties
# so the embedded Kafka broker (a) uses the conventional 9092/2181
# port pair instead of Atlas's built-in 9027/9026 defaults, and (b)
# advertises an address that external clients can actually reach.

set -euo pipefail

CONF="${ATLAS_HOME}/conf/atlas-application.properties"

# Atlas's non-standard solr port, wired into atlas_config.py as
# DEFAULT_SOLR_PORT="9838". We poll this directly to gate readiness.
SOLR_PORT=9838
# When solr is started with `-s /opt/atlas/data/solr`, its default log
# location is `${SOLR_HOME}/../logs/solr.log`, i.e. under
# /opt/atlas/data/logs, NOT /opt/atlas/solr/server/logs.
SOLR_LOG="${ATLAS_DATA_DIR}/logs/solr.log"
APPLICATION_LOG="${ATLAS_LOG_DIR}/application.log"

# ── Startup timing instrumentation ───────────────────────────────────
# Bash's SECONDS builtin counts whole seconds since the shell started,
# i.e. since this entrypoint began. We snapshot it at the top of each
# stage and log the delta when the stage's readiness gate passes, plus a
# grand total at the end. This turns "startup is slow" into per-stage
# numbers visible in `docker logs`, with zero external dependencies.
BOOT_T0=$SECONDS
# Emit "[timing] <label>: <n>s (total <m>s)" where <n> is elapsed since
# the passed-in start snapshot and <m> is elapsed since boot.
mark_timing() {
  local label="$1" start="$2"
  echo "[entrypoint] [timing] ${label}: $(( SECONDS - start ))s (total $(( SECONDS - BOOT_T0 ))s)"
}

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

# Dump the last N lines of any log we can find, for a fail-fast diagnostic.
dump_diagnostic_logs() {
  echo "[entrypoint] ---- last 60 lines of solr.log ($SOLR_LOG) ----"
  tail -60 "$SOLR_LOG" 2>/dev/null || echo "(no solr.log found)"
  # Solr may also log to the older location depending on version.
  if [[ -f "${ATLAS_HOME}/solr/server/logs/solr.log" ]]; then
    echo "[entrypoint] ---- last 40 lines of $ATLAS_HOME/solr/server/logs/solr.log ----"
    tail -40 "${ATLAS_HOME}/solr/server/logs/solr.log" 2>/dev/null || true
  fi
  echo "[entrypoint] ---- last 60 lines of Atlas application.log ----"
  tail -60 "$APPLICATION_LOG" 2>/dev/null || echo "(no application.log found)"
  # atlas_start.py's runProcess dumps solr's stdout/stderr into
  # /opt/atlas/logs/atlas.YYYYMMDD-HHMMSS.{out,err}. Whichever is
  # newest is usually the interesting one.
  local latest_out
  latest_out=$(ls -1t "${ATLAS_LOG_DIR}"/atlas.*.out 2>/dev/null | head -1)
  if [[ -n "$latest_out" ]]; then
    echo "[entrypoint] ---- last 40 lines of $latest_out ----"
    tail -40 "$latest_out" 2>/dev/null || true
  fi
  local latest_err
  latest_err=$(ls -1t "${ATLAS_LOG_DIR}"/atlas.*.err 2>/dev/null | head -1)
  if [[ -n "$latest_err" && -s "$latest_err" ]]; then
    echo "[entrypoint] ---- last 20 lines of $latest_err ----"
    tail -20 "$latest_err" 2>/dev/null || true
  fi
}

# Poll a URL until it returns 2xx or the deadline elapses. Passes
# stderr from curl through, so DNS/connect errors surface in the log.
wait_for_http_ok() {
  local url="$1" deadline_seconds="$2" what="$3"
  local i
  for i in $(seq 1 "$deadline_seconds"); do
    if curl -sf "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "[entrypoint] timeout: $what did not come up within ${deadline_seconds}s (URL: $url)"
  return 1
}

# Wait until a TCP port on localhost accepts a connection.
wait_for_port() {
  local port="$1" deadline_seconds="$2" what="$3"
  local i
  for i in $(seq 1 "$deadline_seconds"); do
    if (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "[entrypoint] timeout: $what did not open port $port within ${deadline_seconds}s"
  return 1
}

# 1. Data-directory policy — the biggest lever on cold-start time.
#
# Atlas's fresh-store bootstrap (registering ~1000 built-in typedefs and
# building their JanusGraph/Solr composite indexes) costs ~130s and runs
# on every boot that starts from an empty store. To avoid paying that on
# every `docker run`, the image ships a *pre-baked seed*: the Dockerfile
# runs this same entrypoint once at build time with ATLAS_SEED_BUILD=true,
# lets Atlas bootstrap, cleanly stops everything, and bakes the resulting
# HBase + Solr state into the image (marked by $SEED_MARKER). At runtime we
# reuse that seed so Atlas finds all types already present and skips the
# bootstrap.
#
# Which dirs are stateful vs volatile:
#   PRESERVE  hbase-root             JanusGraph storage: typedefs, schema,
#                                    composite indexes, audit table
#   PRESERVE  hbase-zookeeper-data   HBase ZK — also holds Solr's cloud
#                                    collection registrations
#   PRESERVE  solr                   Solr cores/indexes for the 3 collections
#   WIPE      kafka, zookeeper       embedded Kafka + its own ZK (topics are
#                                    recreated on boot)
#   WIPE      logs, *.pid            regenerated; stale pids block restart
SEED_MARKER="${ATLAS_DATA_DIR}/.seeded"

# Drop only the volatile bits. Safe to run in every mode.
wipe_volatile() {
  rm -rf "${ATLAS_DATA_DIR}"/kafka \
         "${ATLAS_DATA_DIR}"/zookeeper \
         "${ATLAS_DATA_DIR}"/logs \
         2>/dev/null || true
  rm -f /tmp/hbase--master.pid /tmp/hbase--master.znode 2>/dev/null || true
  rm -f "${ATLAS_LOG_DIR}/atlas.pid" 2>/dev/null || true
  # Clear accumulated logs so each boot starts with a clean, accurate log
  # set. Also ensures the seed-build's final wipe_volatile drops build-time
  # logs from the image instead of shipping them (application.log lives in
  # ATLAS_LOG_DIR, which is outside the data dirs wiped above).
  rm -f "${ATLAS_LOG_DIR}"/*.log "${ATLAS_LOG_DIR}"/*.out "${ATLAS_LOG_DIR}"/*.err 2>/dev/null || true
  rm -rf "${ATLAS_HOME}"/hbase/logs/* 2>/dev/null || true
}

# Drop everything, forcing a from-scratch bootstrap on the next start.
wipe_all() {
  if [[ -d "${ATLAS_DATA_DIR}" ]]; then
    rm -rf "${ATLAS_DATA_DIR}"/hbase-root \
           "${ATLAS_DATA_DIR}"/hbase-zookeeper-data \
           "${ATLAS_DATA_DIR}"/solr \
           2>/dev/null || true
  fi
  wipe_volatile
  rm -f "$SEED_MARKER" 2>/dev/null || true
}

if [[ "${ATLAS_SEED_BUILD:-false}" == "true" ]]; then
  # Build-time seed generation: always start from a clean store.
  echo "[entrypoint] SEED BUILD mode: fresh bootstrap, then snapshot and exit."
  wipe_all
elif [[ "${ATLAS_PERSIST_DATA:-false}" == "true" ]]; then
  # User opted to keep everything (e.g. a mounted volume with their data).
  echo "[entrypoint] persist mode: keeping all data under ${ATLAS_DATA_DIR}."
elif [[ "${ATLAS_FRESH_BOOTSTRAP:-false}" == "true" ]]; then
  # Escape hatch: ignore the baked seed and bootstrap from scratch (~130s).
  echo "[entrypoint] fresh-bootstrap requested: wiping all state."
  wipe_all
elif [[ -f "$SEED_MARKER" ]]; then
  # Normal runtime path in the baked image: reuse the seed, drop volatiles.
  echo "[entrypoint] reusing baked seed — skipping the ~130s typedef bootstrap."
  wipe_volatile
else
  # No seed present (e.g. an empty mounted volume shadowing the image seed).
  echo "[entrypoint] no seed found: bootstrapping from scratch."
  wipe_all
fi

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

# ─────────────────────────────────────────────────────────────────────
# Stage A: HBase.
#
# Render Atlas's hbase-site.xml from its template (the template has
# ${atlas_data} in it, needs runtime substitution), then start HBase
# with the stock start-hbase.sh — that's what atlas_start.py would do
# too, minus the wrapper.
# ─────────────────────────────────────────────────────────────────────
echo "[entrypoint] starting HBase..."
HBASE_T0=$SECONDS
mkdir -p "${ATLAS_LOG_DIR}" "${ATLAS_DATA_DIR}"

# One-off runtime substitution of Atlas's hbase-site template into
# HBase's live conf dir. Same three placeholders atlas_config.configure_hbase
# fills in.
HBASE_TMPL="${ATLAS_HOME}/conf/hbase/hbase-site.xml.template"
HBASE_SITE="${ATLAS_HOME}/hbase/conf/hbase-site.xml"
if [[ -f "$HBASE_TMPL" ]]; then
  sed -e "s|\${hbase_home}|${ATLAS_HOME}|g" \
      -e "s|\${atlas_data}|${ATLAS_DATA_DIR}|g" \
      -e "s|\${url_prefix}|file://|g" \
      "$HBASE_TMPL" > "$HBASE_SITE"
  # atlas_config.configure_hbase() removes the template after rendering,
  # so subsequent boots re-use $HBASE_SITE as-is. Follow the same
  # convention so re-render on ATLAS_PERSIST_DATA=true reboots is a no-op.
  rm -f "$HBASE_TMPL"
fi

"${ATLAS_HOME}/hbase/bin/start-hbase.sh" 2>&1 || {
  echo "[entrypoint] start-hbase.sh returned non-zero."
  dump_diagnostic_logs
  tail -40 "${ATLAS_HOME}"/hbase/logs/hbase--master-*.log 2>/dev/null || true
  exit 1
}

# HBase writes its master pid into /tmp/hbase--master.pid. Verify.
if [[ ! -s /tmp/hbase--master.pid ]] || ! kill -0 "$(cat /tmp/hbase--master.pid)" 2>/dev/null; then
  echo "[entrypoint] HBase master pid file missing or process gone."
  dump_diagnostic_logs
  echo "[entrypoint] ---- hbase master log ----"
  tail -40 "${ATLAS_HOME}"/hbase/logs/hbase--master-*.log 2>/dev/null || true
  exit 1
fi

# HBase's embedded ZooKeeper on 2181 must be reachable before Solr can
# register with it in cloud mode. Wait up to 60s.
if ! wait_for_port 2181 60 "HBase ZooKeeper"; then
  dump_diagnostic_logs
  tail -40 "${ATLAS_HOME}"/hbase/logs/hbase--master-*.log 2>/dev/null || true
  exit 1
fi
echo "[entrypoint] HBase ZooKeeper is up on 2181."
mark_timing "HBase ready" "$HBASE_T0"

# ─────────────────────────────────────────────────────────────────────
# Stage B: Solr.
#
# atlas_start.py normally launches Solr with `bin/solr start -c -z … -p
# 9838 -s /opt/atlas/data/solr`, doesn't check the return code, and
# unconditionally prints "Local Solr started!". We do the same call
# ourselves, then actively wait for the admin endpoint.
# ─────────────────────────────────────────────────────────────────────
echo "[entrypoint] starting Solr on port ${SOLR_PORT}..."
SOLR_T0=$SECONDS
mkdir -p "${ATLAS_DATA_DIR}/solr"
if [[ ! -f "${ATLAS_DATA_DIR}/solr/solr.xml" ]]; then
  cp "${ATLAS_HOME}/solr/server/solr/solr.xml" "${ATLAS_DATA_DIR}/solr/solr.xml"
fi

# `-force` lets Solr run as UID 1000 (the atlas user) without warning.
# `-noprompt` is not needed for `start`; the exit code is what matters.
# We do NOT rely on Solr's own lsof-based wait — we poll admin/info
# ourselves below.
"${ATLAS_HOME}/solr/bin/solr" start \
    -c \
    -z localhost:2181 \
    -p "${SOLR_PORT}" \
    -s "${ATLAS_DATA_DIR}/solr" \
    -force >/tmp/solr-start.out 2>&1 || {
  echo "[entrypoint] 'solr start' returned non-zero:"
  cat /tmp/solr-start.out
  dump_diagnostic_logs
  exit 1
}

echo "[entrypoint] waiting for Solr admin endpoint..."
if ! wait_for_http_ok "http://localhost:${SOLR_PORT}/solr/admin/info/system?wt=json" 120 "Solr"; then
  cat /tmp/solr-start.out || true
  dump_diagnostic_logs
  exit 1
fi
echo "[entrypoint] Solr is up on ${SOLR_PORT}."
mark_timing "Solr ready" "$SOLR_T0"
COLL_T0=$SECONDS

# Create the three Atlas collections. The old path ran `solr create`
# three times in sequence — each invocation spawned its own cold JVM and
# each raced the overseer with a 5s-sleep retry loop. Instead we upload
# the shared configset to ZooKeeper once (a single JVM launch), then fire
# all three CREATE calls concurrently over HTTP. A synchronous CREATE
# returns only once the collection is live, so `wait`-ing on the three
# background curls gives us the same readiness guarantee in ~1/3 the time.
SOLR_CONFIGSET="atlas_config"

# Upload the configset once. Retry briefly to absorb an overseer race on
# a freshly-started cloud Solr.
upconfig_ok=false
for i in $(seq 1 5); do
  if "${ATLAS_HOME}/solr/bin/solr" zk upconfig \
       -z localhost:2181 \
       -n "${SOLR_CONFIGSET}" \
       -d "${ATLAS_HOME}/conf/solr" >>/tmp/solr-create.out 2>&1; then
    upconfig_ok=true
    break
  fi
  echo "[entrypoint] configset upload attempt $i failed, retrying..."
  sleep 3
done
if [[ "$upconfig_ok" != "true" ]]; then
  echo "[entrypoint] failed to upload Solr configset ${SOLR_CONFIGSET}."
  cat /tmp/solr-create.out || true
  dump_diagnostic_logs
  exit 1
fi

# Create one collection via the Collections API, referencing the shared
# configset. Skips if it already exists (e.g. ATLAS_PERSIST_DATA=true
# reboot). Retries to absorb the overseer race on the first CREATE.
create_collection() {
  local name="$1" i
  if curl -sf "http://localhost:${SOLR_PORT}/solr/admin/collections?action=LIST&wt=json" 2>/dev/null \
       | grep -q "\"$name\""; then
    echo "[entrypoint] collection $name already exists."
    return 0
  fi
  for i in $(seq 1 5); do
    if curl -sf "http://localhost:${SOLR_PORT}/solr/admin/collections?action=CREATE&name=${name}&numShards=1&replicationFactor=1&collection.configName=${SOLR_CONFIGSET}&wt=json" \
         >>/tmp/solr-create.out 2>&1; then
      echo "[entrypoint] created collection $name."
      return 0
    fi
    echo "[entrypoint] collection $name create attempt $i failed, retrying..."
    sleep 3
  done
  echo "[entrypoint] failed to create Solr collection $name."
  return 1
}

# Fire all three concurrently, then reap them.
coll_pids=()
for coll in vertex_index edge_index fulltext_index; do
  create_collection "$coll" &
  coll_pids+=($!)
done
coll_failed=false
for pid in "${coll_pids[@]}"; do
  wait "$pid" || coll_failed=true
done
if [[ "$coll_failed" == "true" ]]; then
  echo "[entrypoint] one or more Solr collections failed to create."
  cat /tmp/solr-create.out || true
  dump_diagnostic_logs
  exit 1
fi
echo "[entrypoint] Solr collections ready."
mark_timing "Solr collections created" "$COLL_T0"

# ─────────────────────────────────────────────────────────────────────
# Stage C: Atlas JVM.
#
# Everything HBase/Solr-related is now verified up. Ask atlas_start.py
# to run with both local-service flags off, so it does nothing except
# launch the Atlas JVM and drop atlas.pid.
# ─────────────────────────────────────────────────────────────────────
echo "[entrypoint] starting Atlas JVM..."
ATLAS_T0=$SECONDS
MANAGE_LOCAL_HBASE=false MANAGE_LOCAL_SOLR=false \
  python3 "${ATLAS_HOME}/bin/atlas_start.py"

ATLAS_PID_FILE="${ATLAS_LOG_DIR}/atlas.pid"
for _ in $(seq 1 30); do
  [[ -s "$ATLAS_PID_FILE" ]] && break
  sleep 1
done
if [[ ! -s "$ATLAS_PID_FILE" ]]; then
  echo "[entrypoint] atlas.pid never appeared — Atlas JVM failed to start."
  dump_diagnostic_logs
  exit 1
fi
ATLAS_PID=$(cat "$ATLAS_PID_FILE")
echo "[entrypoint] Atlas JVM pid=${ATLAS_PID}"
mark_timing "Atlas JVM launched" "$ATLAS_T0"
REST_T0=$SECONDS

# ─────────────────────────────────────────────────────────────────────
# Stage D: Atlas REST readiness.
# ─────────────────────────────────────────────────────────────────────
echo "[entrypoint] waiting for Atlas REST on port 21000..."
ATLAS_READY=false
for _ in $(seq 1 240); do
  if curl -sf -u admin:admin "http://localhost:21000/api/atlas/admin/version" >/dev/null 2>&1; then
    ATLAS_READY=true
    break
  fi
  if ! kill -0 "${ATLAS_PID}" 2>/dev/null; then
    echo "[entrypoint] Atlas JVM (pid=${ATLAS_PID}) exited before becoming ready."
    dump_diagnostic_logs
    exit 1
  fi
  sleep 1
done
if [[ "$ATLAS_READY" != "true" ]]; then
  echo "[entrypoint] Atlas REST API did not respond within 240s."
  dump_diagnostic_logs
  kill "${ATLAS_PID}" 2>/dev/null || true
  exit 1
fi
mark_timing "Atlas REST ready" "$REST_T0"
echo "[entrypoint] Atlas is up: http://localhost:21000  (admin/admin)"
echo "[entrypoint] Kafka reachable at ${KAFKA_ADVERTISED_HOST}:${KAFKA_ADVERTISED_PORT}"
echo "[entrypoint] [timing] ===== total startup: $(( SECONDS - BOOT_T0 ))s ====="

# ─────────────────────────────────────────────────────────────────────
# Seed-build mode: Atlas is fully bootstrapped. Stop everything in
# reverse dependency order — Atlas JVM, then Solr (which deregisters its
# ephemeral znodes from HBase's ZK), then HBase (which flushes memstore
# and removes its own ephemerals) — so the snapshot on disk is a clean,
# consistent seed with no stale ZK session state. Then drop the volatile
# dirs, mark the seed, and exit 0 so the Docker build layer captures it.
# ─────────────────────────────────────────────────────────────────────
if [[ "${ATLAS_SEED_BUILD:-false}" == "true" ]]; then
  echo "[entrypoint] seed build: Atlas ready — performing clean ordered shutdown."
  MANAGE_LOCAL_HBASE=false MANAGE_LOCAL_SOLR=false \
    python3 "${ATLAS_HOME}/bin/atlas_stop.py" 2>/dev/null || true
  for _ in $(seq 1 60); do
    kill -0 "${ATLAS_PID}" 2>/dev/null || break
    sleep 1
  done
  echo "[entrypoint] seed build: stopping Solr..."
  "${ATLAS_HOME}/solr/bin/solr" stop -p "${SOLR_PORT}" 2>/dev/null || true
  echo "[entrypoint] seed build: stopping HBase..."
  "${ATLAS_HOME}/hbase/bin/stop-hbase.sh" 2>/dev/null || true
  for _ in $(seq 1 60); do
    if [[ -s /tmp/hbase--master.pid ]] && kill -0 "$(cat /tmp/hbase--master.pid)" 2>/dev/null; then
      sleep 1
    else
      break
    fi
  done
  # Keep the bootstrapped HBase + Solr state; drop everything volatile so
  # it doesn't ride into the image and collide with a fresh boot.
  wipe_volatile
  touch "$SEED_MARKER"
  echo "[entrypoint] seed created under ${ATLAS_DATA_DIR} (marker: ${SEED_MARKER})."
  exit 0
fi

# Forward SIGTERM/SIGINT so `docker stop` shuts down cleanly. atlas_stop.py
# handles HBase and Solr as well.
trap 'echo "[entrypoint] stopping Atlas..."; \
      python3 "${ATLAS_HOME}/bin/atlas_stop.py" 2>/dev/null || true; \
      "${ATLAS_HOME}/solr/bin/solr" stop -p "${SOLR_PORT}" 2>/dev/null || true; \
      "${ATLAS_HOME}/hbase/bin/stop-hbase.sh" 2>/dev/null || true; \
      kill "${ATLAS_PID}" 2>/dev/null || true' TERM INT

# Block on the Atlas JVM. `tail --pid` exits the moment the JVM does,
# propagating that to PID 1 so `docker stop` / crash handling work.
tail --pid="${ATLAS_PID}" -f /dev/null &
wait $!
