FROM apache/fluss:0.9.1-incubating

ARG PAIMON_S3_VERSION=1.3.1

RUN set -eux; \
    curl -L -o /opt/fluss/plugins/paimon/paimon-s3-${PAIMON_S3_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/paimon/paimon-s3/${PAIMON_S3_VERSION}/paimon-s3-${PAIMON_S3_VERSION}.jar

# Ensure Prometheus plugin is properly set up (dir may already have correct ownership)
RUN if [ -d /opt/flink/plugins/metrics-prometheus ]; then \
      chown -R flink:flink /opt/flink/plugins/metrics-prometheus/; \
    fi
