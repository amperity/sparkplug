FROM java:8-jre-alpine

RUN apk update
RUN apk add ca-certificates wget bash procps coreutils
RUN update-ca-certificates

RUN mkdir -p /opt
WORKDIR /opt

ARG HADOOP_VERSION
RUN wget http://apache.mirrors.lucidnetworks.net/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzvf hadoop-${HADOOP_VERSION}.tar.gz && \
    rm hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} hadoop

ARG SPARK_VERSION
ARG SPARK_VARIANT
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${SPARK_VARIANT}.tgz && \
    tar -xzvf spark-${SPARK_VERSION}-bin-${SPARK_VARIANT}.tgz && \
    rm spark-${SPARK_VERSION}-bin-${SPARK_VARIANT}.tgz && \
    mv spark-${SPARK_VERSION}-bin-${SPARK_VARIANT} spark

ENV SPARK_HOME /opt/spark

ADD entry.sh /opt
ENTRYPOINT ["/opt/entry.sh"]
