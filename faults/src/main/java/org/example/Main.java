package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello, World!");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        var source = KafkaSource.<String>builder()
                .setBootstrapServers("redpanda-0:9092") //redpanda-0:9092, localhost:19092
                .setTopics("test")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        var kafkaData = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        kafkaData.print();

        env.execute("Dedup Job");
    }
}