# Spark History Server for viewing Glue session event logs (Glue 4.x / 5.x).
# Build:  docker build -t glue-sparkui .
# Run:    see OBSERVABILITY_SETUP.txt STEP 4 (Windows/PowerShell commands).
#
# LOG_DIR accepts either
#   file:/logs                    (mount locally-synced logs at /logs)
#   s3a://<bucket>/spark-events/  (direct S3 read; pass AWS_* env credentials)

FROM apache/spark:3.5.1

USER root

# S3 support: hadoop-aws must match the Hadoop client bundled with the image (3.3.4)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/hadoop-aws-3.3.4.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

ENV LOG_DIR=file:/logs
# keep the history server in the foreground so the container stays up
ENV SPARK_NO_DAEMONIZE=true

EXPOSE 18080

CMD ["/bin/bash", "-c", "\
  SPARK_HISTORY_OPTS=\"\
    -Dspark.history.fs.logDirectory=${LOG_DIR} \
    -Dspark.history.fs.update.interval=15s \
    -Dspark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
    ${EXTRA_HISTORY_OPTS}\" \
  /opt/spark/sbin/start-history-server.sh"]
