FROM --platform=linux/x86_64 sanketikahub/flink:1.15.2-scala_2.12-jdk-11 as kafka-connector-image
USER flink
COPY ./kafka-connector/target/kafka-connector-1.0.0.jar $FLINK_HOME/lib