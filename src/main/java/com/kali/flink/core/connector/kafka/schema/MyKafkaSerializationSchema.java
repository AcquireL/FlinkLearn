package com.kali.flink.core.connector.kafka.schema;

import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

public class MyKafkaSerializationSchema implements KafkaSerializationSchema<String> {

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {

    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String value, @Nullable Long timestamp) {
        String topic="mykafka2";
        byte[] valueByte=value.getBytes();
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<byte[], byte[]>(topic,valueByte);
        return producerRecord;
    }


}
