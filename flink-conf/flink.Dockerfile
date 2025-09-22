FROM flink:1.20.2-java11
USER root
RUN apt-get update && apt-get install -y python3 python3-pip python3-venv && \
    ln -s /usr/bin/python3 /usr/bin/python && rm -rf /var/lib/apt/lists/*

ARG FLINK_JARS_DIR=/opt/flink/lib

# MongoDB CDC
RUN wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mongodb-cdc/3.4.0/flink-sql-connector-mongodb-cdc-3.4.0.jar

# Kafka connector
RUN wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar

# Elasticsearch connector
RUN wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.1.0-1.20/flink-sql-connector-elasticsearch7-3.1.0-1.20.jar

# Apache Iceberg connector for Flink 
RUN wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.7.2/iceberg-flink-runtime-1.20-1.7.2.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/3.1.3/hive-metastore-3.1.3.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/hive/hive-common/3.1.3/hive-common-3.1.3.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/thrift/libthrift/0.13.0/libthrift-0.13.0.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/3.1.3/hive-standalone-metastore-3.1.3.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/hive/hive-serde/3.1.3/hive-serde-3.1.3.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.2/postgresql-42.7.2.jar

# Delta Lake
RUN wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/io/delta/delta-flink/3.2.1/delta-flink-3.2.1.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/io/delta/delta-standalone_2.12/3.2.1/delta-standalone_2.12-3.2.1.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/com/chuusai/shapeless_2.12/2.3.12/shapeless_2.12-2.3.12.jar


# S3 and Hadoop dependencies
RUN wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.20.2/flink-s3-fs-hadoop-1.20.2.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.787/aws-java-sdk-bundle-1.12.787.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.4/hadoop-mapreduce-client-core-3.3.4.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.4/hadoop-hdfs-client-3.3.4.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs/3.3.4/hadoop-hdfs-3.3.4.jar


# Parquet format 
RUN wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/1.20.2/flink-parquet-1.20.2.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop/1.13.1/parquet-hadoop-1.13.1.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/parquet/parquet-column/1.13.1/parquet-column-1.13.1.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/parquet/parquet-common/1.13.1/parquet-common-1.13.1.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/parquet/parquet-encoding/1.13.1/parquet-encoding-1.13.1.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/parquet/parquet-format-structures/1.13.1/parquet-format-structures-1.13.1.jar && \
    wget -P ${FLINK_JARS_DIR} https://repo1.maven.org/maven2/org/apache/parquet/parquet-jackson/1.13.1/parquet-jackson-1.13.1.jar

WORKDIR /opt/flink
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy Flink configuration
COPY flink-conf/flink-config.yml /opt/flink/conf/flink-conf.yaml

# Copy Hadoop configuration
COPY flink-conf/core-site.xml /opt/flink/conf/core-site.xml
ENV HADOOP_CONF_DIR=/opt/flink/conf

# Copy Hive configuration
COPY hive-conf/hive-site.xml /opt/flink/conf/hive-site.xml