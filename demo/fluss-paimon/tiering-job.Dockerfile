FROM apache/fluss-quickstart-flink:1.20-0.9.1-incubating

ARG PAIMON_VERSION=1.3.1

RUN set -eux; \
    curl -L -o /opt/flink/lib/paimon-flink-1.20-${PAIMON_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.20/${PAIMON_VERSION}/paimon-flink-1.20-${PAIMON_VERSION}.jar; \
    curl -L -o /opt/flink/lib/paimon-s3-${PAIMON_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/paimon/paimon-s3/${PAIMON_VERSION}/paimon-s3-${PAIMON_VERSION}.jar; \
    curl -L -o /opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar; \
    cp -f /opt/flink/paimon/fluss-lake-paimon-*.jar /opt/flink/lib/; \
    cp -f /opt/flink/paimon/hadoop-apache-*.jar /opt/flink/lib/; \
    chown flink:flink /opt/flink/lib/paimon-*.jar /opt/flink/lib/flink-shaded-hadoop-*.jar /opt/flink/lib/fluss-lake-paimon-*.jar /opt/flink/lib/hadoop-apache-*.jar

RUN ls -la /opt/flink/lib/paimon-* /opt/flink/lib/flink-shaded-hadoop-* /opt/flink/lib/fluss-lake-paimon-* /opt/flink/lib/hadoop-apache-*
