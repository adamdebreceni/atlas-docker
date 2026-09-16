# syntax=docker/dockerfile:1.6
#
# Single self-contained Apache Atlas image.
#
# Stage 1 clones apache/atlas at tag release-2.5.0 and builds the
# distribution with the embedded-hbase-solr profile so the resulting
# tarball ships HBase and Solr alongside Atlas. Atlas also starts an
# embedded Kafka broker + embedded ZooKeeper at runtime, so a single
# container is enough to serve the REST API and the notification bus.
#
# Stage 2 is a slim JRE + python3 image that just extracts and runs
# the built distribution. No maven, no source, no .m2 cache.

########################  Stage 1: build  ###############################
FROM maven:3.8-eclipse-temurin-8 AS builder

ARG ATLAS_VERSION=2.5.0
ARG ATLAS_GIT_REF=release-${ATLAS_VERSION}

# -Xms/-Xmx: standard heap sizing for the Atlas build.
# -Dmaven.wagon.http.retryHandler.class=standard + retryHandler.count=5:
#     Auto-retry HTTP transfers on transient errors (Maven Central occasionally
#     truncates responses, which otherwise fails the entire ~45-min build).
# -Daether.connector.resumeDownloads=true:
#     Resume partial downloads instead of restarting them from zero.
ENV MAVEN_OPTS="-Xms2g -Xmx2g \
    -Dmaven.wagon.http.retryHandler.class=standard \
    -Dmaven.wagon.http.retryHandler.count=5 \
    -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 \
    -Daether.connector.resumeDownloads=true \
    -Daether.connector.resumeThreshold=32768"

RUN apt-get update \
 && apt-get install -y --no-install-recommends git python3 patch \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN git clone --depth 1 --branch ${ATLAS_GIT_REF} https://github.com/apache/atlas.git .

# Pre-fetched HBase and Solr distributions. The embedded-hbase-solr profile
# would normally download these from archive.apache.org during the final
# `atlas-distro` packaging step, but archive.apache.org is slow (~200 KB/s)
# and the Ant `<get>` task offers no retry — a mid-build network blip after
# a 45-minute mvn compile is expensive to recover from. So we ship the two
# tarballs into the build context under `prefetch/` and let the Ant tasks
# skip their downloads via skipexisting="true".
#
# See README.md for how to prefetch them (single curl each into the paths
# below). Both files are gitignored — they are inputs, not sources.
COPY prefetch/hbase/hbase-2.6.4.tar.gz distro/hbase/hbase-2.6.4.tar.gz
COPY prefetch/solr/solr-8.11.3.tgz     distro/solr/solr-8.11.3.tgz

# Route all artifact resolution through Maven Central. Several of the
# transitive POMs in Atlas 2.5.0 point at dead/slow repos (apache
# staging, jboss, java.net, typesafe, ...) which can each cost 60s+
# per artifact when they time out. Without this mirror, a full build
# can hang for hours in dependency resolution.
COPY maven-settings.xml /root/.m2/settings.xml

# Build the distribution tarball with embedded HBase + Solr bundled in.
# -DskipTests trims a big chunk of build time; -Drat.skip skips the
# Apache RAT license check that occasionally trips on shallow clones.
#
# BuildKit cache mount on /root/.m2 preserves the Maven local repository
# across rebuilds — if a build fails on a flaky Maven Central download,
# the next attempt reuses everything already fetched and only pulls the
# missing jar.
#
# javax.jms:jms:1.1 shim: falcon-bridge-shim transitively depends on
# Storm which pulls in `javax.jms:jms:1.1`. Sun never published that
# JAR to Maven Central (only the POM), and its historical home on
# java.net is gone. We install the API-compatible geronimo-jms_1.1_spec
# JAR into the local repo under the `javax.jms:jms:1.1` GAV so the
# transitive resolve succeeds. `|| true` tolerates a warm cache where
# the artifact is already installed from a previous build attempt.
RUN --mount=type=cache,target=/root/.m2/repository,sharing=locked \
    mvn -B -q dependency:get \
        -Dartifact=org.apache.geronimo.specs:geronimo-jms_1.1_spec:1.1.1 \
 && mvn -B -q install:install-file \
        -Dfile=/root/.m2/repository/org/apache/geronimo/specs/geronimo-jms_1.1_spec/1.1.1/geronimo-jms_1.1_spec-1.1.1.jar \
        -DgroupId=javax.jms -DartifactId=jms -Dversion=1.1 -Dpackaging=jar \
 && mvn -B clean package -Pdist,embedded-hbase-solr -DskipTests -Drat.skip=true

# Extract the -bin tarball here so the runtime stage COPYs a plain tree.
RUN tar -xzf distro/target/apache-atlas-${ATLAS_VERSION}-bin.tar.gz -C /opt \
 && mv /opt/apache-atlas-${ATLAS_VERSION} /opt/atlas

########################  Stage 2: runtime  #############################
# Note: JDK (not JRE) — Atlas's atlas_start.py invokes the `jar` command
# at container start to expand the WAR file into an exploded webapp, and
# `jar` ships only with the JDK, not the JRE. Trying a JRE base fails
# with `No such file or directory: '/opt/java/openjdk/bin/jar'`.
FROM eclipse-temurin:8-jdk-jammy

ARG ATLAS_VERSION=2.5.0

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      python3 \
      netcat-openbsd \
      procps \
      curl \
      tini \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/atlas /opt/atlas
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Solr refuses to start as root ("Starting Solr as the root user is a
# security risk"), and there is no clean way to override that from
# atlas_start.py — it invokes `solr start` without `-force`. Run the
# whole Atlas stack as a dedicated non-root user. `logs/` and `data/`
# have to be writable by that user for HBase, Solr, and Atlas.
RUN useradd -r -u 1000 -m -d /home/atlas -s /bin/bash atlas \
 && chown -R atlas:atlas /opt/atlas
USER atlas

ENV ATLAS_HOME=/opt/atlas \
    ATLAS_LOG_DIR=/opt/atlas/logs \
    ATLAS_DATA_DIR=/opt/atlas/data \
    ATLAS_SERVER_HEAP="-Xms1g -Xmx2g" \
    KAFKA_ADVERTISED_HOST=localhost \
    KAFKA_ADVERTISED_PORT=9092 \
    ATLAS_VERSION=${ATLAS_VERSION}

# 21000 = Atlas REST API + UI
# 9092  = embedded Kafka broker (advertised to external clients)
# Note: Kafka's own ZooKeeper lives inside the container on 9026 and is
# not exposed — external clients don't need it because they use bootstrap
# discovery. HBase runs its own separate ZooKeeper on 2181 for its
# coordination, also container-internal.
EXPOSE 21000 9092

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
