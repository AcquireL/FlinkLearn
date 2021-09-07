package com.kali.flink.core.connector.hdfs;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Properties;
import java.util.regex.Pattern;

public class HdfsSinkParquet {
    private static final Logger logger = LoggerFactory.getLogger(HdfsSinkParquet.class);
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    private void useStreamingFileSink() throws Exception {
        env.setParallelism(1);
        env.enableCheckpointing(60 * 1000L);
        env.setStateBackend((StateBackend) new FsStateBackend("hdfs://learn:9000/checkPoint/flinkDemo"));
        // 动态获取kafka中的数据
        Properties props = new Properties();
        String broker_list = "learn:9092";
        // String broker_list="learn:9092";
        props.setProperty("bootstrap.servers", broker_list);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("group.id", "trafficwisdom-streaming");
        props.put("enable.auto.commit", false);
        props.put("max.poll.records", 1000);
        props.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "1000");
        String topicName = "VDF_TEST2";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                topicName, new SimpleStringSchema(), props);
        consumer.setStartFromLatest();
        DataStream<String> stream = env.addSource(consumer);

        // 使用StreamingFileSink写入hdfs

        StreamingFileSink<String> sink = StreamingFileSink
                .forBulkFormat(new Path("hdfs://learn:9000/data/output/parquet"),
                        ParquetAvroWriters.forReflectRecord(String.class)
                )
                .withBucketAssigner(new DateTimeBucketAssigner())
                .withBucketCheckInterval(60 * 1000L)
                .build();
        stream.addSink(sink);

        stream.print();

        env.execute("Test use Streaming File Sink");
    }

    public static void main(String[] args) throws Exception {
        HdfsSinkParquet hdfsSinkParquet = new HdfsSinkParquet();
        hdfsSinkParquet.useStreamingFileSink();
    }
}
