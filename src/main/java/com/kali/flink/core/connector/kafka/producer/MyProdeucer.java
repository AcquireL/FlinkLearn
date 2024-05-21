package com.kali.flink.core.connector.kafka.producer;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;


public class MyProdeucer {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//        DataStreamSource<String> stream = env.socketTextStream("acquirel", 9001);
        DataStreamSource<String> stream = env.fromElements("1","2");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "acquirel:9092");

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("test", new SimpleStringSchema(), properties);
        stream.print();
        stream.addSink(kafkaProducer);


        env.execute("Flink Kafka Producer Example");
    }
}
