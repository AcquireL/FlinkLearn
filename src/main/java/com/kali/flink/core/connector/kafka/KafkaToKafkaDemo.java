package com.kali.flink.core.connector.kafka;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingWindowReservoir;
import com.kali.flink.core.connector.kafka.schema.MyKafkaDeserializationSchema;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class KafkaToKafkaDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        List<String> list =new LinkedList<String>();
        env.enableCheckpointing(100000);
        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend("file:///D:/GitSpace/git-hub/FlinkLearn"));
        } else {
            env.setStateBackend(new FsStateBackend("hdfs://learn:9000/flink-checkpoint/checkpoint"));
        }
        //准备kafka连接参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "learn:9092");//集群地址
        props.setProperty("group.id", "flink");//消费者组id
        props.setProperty("auto.offset.reset", "latest");//latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        props.setProperty("enable.auto.commit", "false");
        // props.setProperty("flink.partition-discovery.interval-millis", "5000");//会开启一个后台线程每隔5s检测一下Kafka的分区情况,实现动态分区检测
        //props.setProperty("enable.auto.commit", "true");//自动提交(提交到默认主题,后续学习了Checkpoint后随着Checkpoint存储在Checkpoint和默认主题中)
        //props.setProperty("auto.commit.interval.ms", "2000");//自动提交的时间间隔
        //使用连接参数创建FlinkKafkaConsumer/kafkaSource
        FlinkKafkaConsumer<Tuple2<Long, String>> kafkaSource = new FlinkKafkaConsumer<>("lwj", new MyKafkaDeserializationSchema(), props);
        //使用kafkaSource
        DataStream<Tuple2<Long, String>> kafkaDS = env.addSource(kafkaSource);
        kafkaDS.print();


        String str="";
        String[] s = str.split(" ");


        // sink Kafka
        Properties produceProps = new Properties();
        produceProps.setProperty("bootstrap.servers", "learn:9092");

        FlinkKafkaProducer<Tuple2<Long, String>> finkKafkaProducer = new FlinkKafkaProducer<Tuple2<Long, String>>("", new KafkaSerializationSchema<Tuple2<Long, String>>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(Tuple2<Long, String> value, @Nullable Long aLong) {
                String topic = "mykafka2";
                byte[] valueByte = value.f1.getBytes();
                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<byte[], byte[]>(topic, valueByte);
                return producerRecord;
            }
        }, produceProps, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        kafkaDS.addSink(finkKafkaProducer);





        // 手动实现metric
        /*kafkaDS.map(new RichMapFunction<Tuple2<Long, String>, Object>() {

            DropwizardHistogramWrapper myHistogram;

            @Override
            public void open(Configuration parameters) throws Exception {

                Histogram histogram =
                        new Histogram(new SlidingWindowReservoir(1000));

                myHistogram = getRuntimeContext()
                        .getMetricGroup()
                        .histogram("myHistogram", new DropwizardHistogramWrapper(histogram));
            }

            @Override
            public Object map(Tuple2<Long, String> value) throws Exception {
                long sourceTs = value.f0;
                long nowTs = System.currentTimeMillis();
                System.out.println(nowTs-sourceTs);
                myHistogram.update(nowTs - sourceTs);

                return null;
            }
        }).name("test");*/

        env.execute();
    }
}
