FROM apache/spark:3.5.4-java17

USER root

ENV ICEBERG_VERSION=1.11.0

RUN cd /opt/spark/jars \
    && curl -fsSL -o iceberg-spark-runtime-3.5_2.12.jar \
         "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    && curl -fsSL -o iceberg-aws-bundle.jar \
         "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    && curl -fsSL -o spark-connect_2.12-3.5.4.jar \
         "https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.5.4/spark-connect_2.12-3.5.4.jar" \
    && curl -fsSL -o hadoop-aws-3.3.4.jar \
         "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    && curl -fsSL -o aws-java-sdk-bundle-1.12.262.jar \
         "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
    && chown 185:185 \
         /opt/spark/jars/iceberg-spark-runtime-3.5_2.12.jar \
         /opt/spark/jars/iceberg-aws-bundle.jar \
         /opt/spark/jars/spark-connect_2.12-3.5.4.jar \
         /opt/spark/jars/hadoop-aws-3.3.4.jar \
         /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

COPY conf/spark-defaults.conf conf/spark-env.sh /opt/spark/conf/
RUN chown 185:185 /opt/spark/conf/spark-defaults.conf /opt/spark/conf/spark-env.sh

COPY jobs/ingest-sessions.py /opt/spark/jobs/ingest-sessions.py
RUN chmod 755 /opt/spark/jobs/ingest-sessions.py

USER 185

WORKDIR /opt/spark