package com.kali.flink.core.connector.kafka.consumer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

/**
 *  定义KafkaConsumer
 */
public class MyKafkaConsumer {

    // 初始化properties
    private static Properties initProperties(){
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "learn:9092");
        consumerProps.setProperty("group.id", "group_vdf_sinkToHbase");
        consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "1000");
        return consumerProps;
    }

    // 使用默认的SimpleStringSchema获取kafkaConsumer
    public static FlinkKafkaConsumer<String> getKafkaConsumer(){
        Properties properties = MyKafkaConsumer.initProperties();
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>("topic1", new SimpleStringSchema(), properties);
        flinkKafkaConsumer.setStartFromLatest();
        return flinkKafkaConsumer;
    }

}
