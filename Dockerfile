# Pre-built JAR — run `mvn package -DskipTests` locally first, then docker compose up
FROM flink:1.19.1-scala_2.12-java11

# Enable S3 plugin (same as Dockerfile.flink-base)
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

COPY target/ais-flink-job1-*.jar /opt/flink/usrlib/ais-flink-job1.jar
COPY docker/submit-job.sh /opt/flink/bin/submit-job.sh
RUN chmod +x /opt/flink/bin/submit-job.sh
