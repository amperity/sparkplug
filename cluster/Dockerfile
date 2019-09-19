FROM java:8-jre-alpine

ARG SPARK_VERSION
ARG SPARK_VARIANT

RUN apk update
RUN apk add ca-certificates wget bash procps coreutils
RUN update-ca-certificates

RUN mkdir /opt
WORKDIR /opt
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${SPARK_VARIANT}.tgz && \
    tar -xzvf spark-${SPARK_VERSION}-bin-${SPARK_VARIANT}.tgz && \
    rm spark-${SPARK_VERSION}-bin-${SPARK_VARIANT}.tgz

ENV SPARK_HOME /opt/spark-${SPARK_VERSION}-bin-${SPARK_VARIANT}
ADD entry.sh /opt
ENTRYPOINT ["/opt/entry.sh"]
