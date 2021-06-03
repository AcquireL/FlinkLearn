package com.kali.flink.core.connector.kafka.schema;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

public class MyKafkaDeserializationSchema implements KafkaDeserializationSchema<Tuple2<Long,String>> {


    @Override
    public Tuple2<Long,String> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        long startTs = System.currentTimeMillis();
        String json = new String(consumerRecord.value());
        Tuple2<Long, String> value = new Tuple2<>(startTs,json);
        return value;
    }

    @Override
    public TypeInformation getProducedType() {
         return TypeInformation.of(new TypeHint<Tuple2<Long,String>>() {
        });
    }

    @Override
    public boolean isEndOfStream(Tuple2<Long, String> integerStringTuple2) {
        return false;
    }
}
