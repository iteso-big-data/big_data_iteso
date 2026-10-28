# Flink + PyFlink image (used by both jobmanager and taskmanager)
# Multi-arch: builds natively on linux/amd64 and linux/arm64 (Apple Silicon).
# All three versions MUST belong to the same Flink release line.
ARG FLINK_VERSION=2.0.1
FROM flink:${FLINK_VERSION}-scala_2.12-java17

ARG FLINK_VERSION=2.0.1
ARG KAFKA_CONNECTOR=4.0.1-2.0
ARG PYTHON_VERSION=3.11
# Set automatically by BuildKit: amd64 | arm64
ARG TARGETARCH

USER root

# JNI headers (~300 KB) from the matching JDK; the base image only has a JRE.
# Only used when PyFlink/pemja must be compiled (arm64), harmless on amd64.
COPY --from=eclipse-temurin:17-jdk /opt/java/openjdk/include /opt/java/openjdk/include

# uv: standalone Python manager (multi-arch image)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/python

# Python venv + PyFlink.
#  - amd64: prebuilt wheels exist -> no compiler needed.
#  - arm64: PyFlink 2.0.x and pemja have no Linux arm64 wheels -> compile from
#    source with gcc, then remove the compiler to keep the image small.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates; \
    if [ "${TARGETARCH}" != "amd64" ]; then \
        apt-get install -y --no-install-recommends build-essential; \
    fi; \
    uv venv --python ${PYTHON_VERSION} /opt/venv; \
    uv pip install --python /opt/venv/bin/python --no-cache "apache-flink==${FLINK_VERSION}"; \
    if [ "${TARGETARCH}" != "amd64" ]; then \
        apt-get purge -y --auto-remove build-essential; \
    fi; \
    rm -rf /var/lib/apt/lists/*

# `python` on PATH is what Flink's Python workers launch by default
ENV PATH="/opt/venv/bin:${PATH}"

# Kafka SQL connector on the cluster classpath (no pipeline.jars needed)
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar && \
    chmod 644 /opt/flink/lib/*.jar

RUN mkdir -p /opt/flink/work-dir/checkpoints
RUN chown -R flink:flink /opt/flink/work-dir/checkpoints
# Sanity check at build time
RUN python -c "import pyflink, pemja, importlib.metadata as m; print('PyFlink', m.version('apache-flink'), 'on', '${TARGETARCH}')"