#!/usr/bin/env bash
# One-shot build wrapper.
#
# Ensures the two large third-party tarballs the Atlas embedded profile
# needs (HBase and Solr) are present under prefetch/, then runs
# `docker build`. Prefetching on the host has proved far more reliable
# than downloading from inside the build container: archive.apache.org
# serves at ~200 KB/s, and the Ant task in distro/pom.xml doesn't retry,
# so a mid-build network blip 40+ minutes in fails the whole build. On
# the host, curl -C - can resume through blips, and once the files exist
# the actual docker build never has to touch archive.apache.org.
#
# Anyone who has already prefetched (or whose CI supplies) the tarballs
# can invoke `docker build -t atlas .` directly and skip this script.

set -euo pipefail

HBASE_VERSION=2.6.4
SOLR_VERSION=8.11.3
IMAGE_TAG="${IMAGE_TAG:-atlas}"

HBASE_URL="https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz"
SOLR_URL="https://archive.apache.org/dist/lucene/solr/${SOLR_VERSION}/solr-${SOLR_VERSION}.tgz"

HBASE_LOCAL="prefetch/hbase/hbase-${HBASE_VERSION}.tar.gz"
SOLR_LOCAL="prefetch/solr/solr-${SOLR_VERSION}.tgz"

mkdir -p "$(dirname "$HBASE_LOCAL")" "$(dirname "$SOLR_LOCAL")"

fetch() {
  local url="$1" out="$2"
  # -C - resumes partial downloads; --retry survives transient errors.
  # If the file is already fully present, curl -C - returns quickly.
  curl -fL --retry 5 --retry-delay 10 --retry-all-errors -C - \
       -o "$out" "$url"
}

if [[ ! -s "$HBASE_LOCAL" ]]; then
  echo "==> fetching HBase ${HBASE_VERSION} (~340 MB, may take a while)"
  fetch "$HBASE_URL" "$HBASE_LOCAL"
fi
if [[ ! -s "$SOLR_LOCAL" ]]; then
  echo "==> fetching Solr ${SOLR_VERSION} (~215 MB, may take a while)"
  fetch "$SOLR_URL" "$SOLR_LOCAL"
fi

echo "==> building image ${IMAGE_TAG}"
exec docker build -t "${IMAGE_TAG}" .
