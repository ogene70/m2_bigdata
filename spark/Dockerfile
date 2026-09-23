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
    && chown 185:185 \
         /opt/spark/jars/iceberg-spark-runtime-3.5_2.12.jar \
         /opt/spark/jars/iceberg-aws-bundle.jar \
         /opt/spark/jars/spark-connect_2.12-3.5.4.jar

COPY conf/spark-defaults.conf conf/spark-env.sh /opt/spark/conf/
RUN chown 185:185 /opt/spark/conf/spark-defaults.conf /opt/spark/conf/spark-env.sh

USER 185

WORKDIR /opt/spark