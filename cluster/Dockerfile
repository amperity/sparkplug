FROM eclipse-temurin:11-jdk

RUN apt update
RUN apt install -yy ca-certificates wget bash procps coreutils python3
RUN update-ca-certificates

RUN mkdir -p /opt
WORKDIR /opt

ARG HADOOP_VERSION
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzvf hadoop-${HADOOP_VERSION}.tar.gz && \
    rm hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} hadoop

ARG SPARK_VERSION
ARG SPARK_VARIANT=without-hadoop
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${SPARK_VARIANT}.tgz && \
    tar -xzvf spark-${SPARK_VERSION}-bin-${SPARK_VARIANT}.tgz && \
    rm spark-${SPARK_VERSION}-bin-${SPARK_VARIANT}.tgz && \
    mv spark-${SPARK_VERSION}-bin-${SPARK_VARIANT} spark

ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ADD spark-env.sh /opt/spark/conf/spark-env.sh

RUN mkdir -p /tmp/spark-events

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P "${SPARK_HOME}/jars/"

ARG AWS_SDK_VERSION="1.12.99"
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P "${SPARK_HOME}/jars/"
