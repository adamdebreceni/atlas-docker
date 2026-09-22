# Apache Atlas — single-container Docker image

A self-contained Docker image that builds Apache Atlas **2.5.0** from
source and runs it with embedded HBase, Solr, Kafka, and ZooKeeper —
all in one container. Exposes the REST API on **21000** and the
embedded Kafka broker on **9092**, so you can drive Atlas from outside
the container using either its HTTP API or its notification topics.

There is no official Atlas Docker image published by Apache; the Atlas
source tree ships a multi-container docker-compose setup instead. This
image collapses that stack into one container for local development
and demos.

## Build

```
./build.sh
```

`build.sh` fetches two large third-party tarballs (HBase 2.6.4 and
Solr 8.11.3, ~500 MB combined) into `prefetch/` if they're not already
there, then runs `docker build -t atlas .`. Prefetching on the host
keeps them out of the mvn/Ant path — `archive.apache.org` is slow
and the Ant `<get>` in Atlas's `distro/pom.xml` doesn't retry, so a
mid-build network blip 40 minutes into the compile would otherwise
throw the whole build away.

If you already have the tarballs at `prefetch/hbase/hbase-2.6.4.tar.gz`
and `prefetch/solr/solr-8.11.3.tgz`, `docker build -t atlas .` works
directly.

Expect the **first build to take 30–60 minutes** and to download
several GB of Maven dependencies plus the HBase and Solr tarballs.
Subsequent builds are cached at the layer level.

## Run

The typical case — Kafka client running on the docker host:

```
docker run --rm -it \
  -p 21000:21000 -p 9092:9092 \
  -e KAFKA_ADVERTISED_HOST=localhost \
  --name atlas atlas
```

Startup takes **2–5 minutes** while HBase, Solr, Kafka, and Atlas all
come up in-process. When ready, you'll see:

```
[entrypoint] Atlas is up: http://localhost:21000  (admin/admin)
```

### If your Kafka client is not on the docker host

Set `KAFKA_ADVERTISED_HOST` to whatever address the client will use to
reach the container — otherwise Kafka's bootstrap protocol will hand
the client back an address it can't connect to, and produce/consume
will hang.

Examples:

- Client in another container on the same host, using the Docker
  bridge: `-e KAFKA_ADVERTISED_HOST=<container-name-or-ip>`.
- Client on a remote machine reaching the docker host by DNS name:
  `-e KAFKA_ADVERTISED_HOST=my-dev-box.internal`.
- Client running on macOS / Windows and connecting *from another
  container*: `-e KAFKA_ADVERTISED_HOST=host.docker.internal`.

## Ports

| Port  | Protocol | Purpose                                      |
|-------|----------|----------------------------------------------|
| 21000 | HTTP     | Atlas REST API and web UI (admin/admin)      |
| 9092  | Kafka    | Embedded Kafka broker (ATLAS_HOOK, ATLAS_ENTITIES) |

Two internal-only ZooKeepers stay inside the container: HBase's on 2181
(for its own coordination) and Kafka's on 9026 (for Kafka's coordination).
External clients don't need them — Kafka's bootstrap protocol advertises
the broker directly.

## Resource requirements

- Give the Docker daemon **at least 6 GB of RAM**. HBase, Solr, Kafka,
  and Atlas each run their own JVM inside the container.
- Persistence: **ephemeral by design**. HBase and Solr write under
  `/opt/atlas/data`; every `docker run` starts fresh. If you want data
  to survive restarts, mount a volume there yourself:
  `-v atlas-data:/opt/atlas/data`.
- Architecture: the image is built for `linux/amd64`. The platform is
  pinned on the `FROM` lines in the `Dockerfile`, so Apple Silicon hosts
  build it under emulation (Rosetta) — no extra flags needed.

## Verification

Once the container is up, all four of these should work from the host:

**1. REST API responds**

```
curl -u admin:admin http://localhost:21000/api/atlas/admin/version
```

Expect a JSON body containing `"Version": "2.5.0"`.

**2. Kafka is reachable and the Atlas topics exist**

```
docker run --rm --network host bitnami/kafka:3.7 \
  kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Expect `ATLAS_HOOK` and `ATLAS_ENTITIES` in the output.

**3. Produce into ATLAS_HOOK**

```
docker run --rm -i --network host bitnami/kafka:3.7 \
  kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic ATLAS_HOOK
```

Paste a valid Atlas hook JSON payload (see the Atlas notification
docs), then confirm the entity landed via the REST API.

**4. Consume from ATLAS_ENTITIES**

```
docker run --rm --network host bitnami/kafka:3.7 \
  kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic ATLAS_ENTITIES --from-beginning
```

Create or update an entity through the REST API and watch a message
land in the consumer.

## Environment variables

| Variable                | Default     | Purpose                                                |
|-------------------------|-------------|--------------------------------------------------------|
| `KAFKA_ADVERTISED_HOST` | `localhost` | Address Kafka hands out to external clients            |
| `KAFKA_ADVERTISED_PORT` | `9092`      | Port Kafka advertises (match your `-p` mapping)        |
| `ATLAS_SERVER_HEAP`     | `-Xms1g -Xmx2g` | JVM heap for the Atlas server process              |
| `ATLAS_LOG_DIR`         | `/opt/atlas/logs` | Where Atlas / HBase / Solr write logs            |
| `ATLAS_PERSIST_DATA`    | `false`     | Set to `true` to keep `/opt/atlas/data` across restarts (only sensible with a mounted volume — see Stability below) |

## Stability

Atlas's upstream `atlas_start.py` starts Solr, HBase, and the JVM as
background daemons and does not check whether Solr or its collections
actually came up before starting the Atlas JVM. If Solr fails (stale
state, IO stall, missing `lsof`, etc.), the Atlas JVM retries JanusGraph
init for ~10 minutes before giving up — a long silent hang that used to
present as "sometimes fails to start".

This image papers over that with two changes:

- **Fail-fast readiness gating.** The entrypoint verifies Solr's admin
  endpoint responds and the three collections (`vertex_index`,
  `edge_index`, `fulltext_index`) exist before waiting on Atlas's REST
  endpoint. Each stage has a bounded timeout; on failure the container
  exits non-zero within ~2 minutes and dumps the last lines of
  `solr.log` and `application.log` to `docker logs`.
- **Ephemeral by default.** On every boot the entrypoint wipes
  `/opt/atlas/data/{hbase-root,solr,kafka,zookeeper}` and any stale
  pid files. `docker start <same-container>` therefore behaves like a
  fresh `docker run`, which is the reliable path. If you've mounted a
  volume you want to keep, set `ATLAS_PERSIST_DATA=true`.

## Credentials

Default UI/REST creds are `admin` / `admin` (from the shipped
`users-credentials.properties`). Change them by editing that file
inside `/opt/atlas/conf` and restarting the container.
