FROM apache/airflow:2.11.2-python3.11

USER root

# Installing Java 17 and curl for Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configuring Java ans Spark paths
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Downloading and Installing Spark 3.5.8
RUN mkdir -p ${SPARK_HOME} && \
    curl -fsSL https://dlcdn.apache.org/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz | \
    tar -xz -C ${SPARK_HOME} --strip-components=1 && \
    chown -R airflow:root ${SPARK_HOME}

# Copying Hadoop AWS and AWS SDK JARs for S3 connectivity
COPY hadoop-aws-3.3.4.jar ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar
COPY aws-java-sdk-bundle-1.12.262.jar ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar

USER airflow

# Installing Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copying S3 JARs to PySpark pip directory (PySpark uses its own jars/ in runtime)
RUN cp /opt/spark/jars/hadoop-aws-3.3.4.jar /home/airflow/.local/lib/python3.11/site-packages/pyspark/jars/ && \
    cp /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar /home/airflow/.local/lib/python3.11/site-packages/pyspark/jars/