package com.kali.flink.core.connector.kafka.producer;

import com.kali.flink.core.connector.kafka.schema.MyKafkaSerializationSchema;
import com.kali.flink.core.metrics.DummyLatencyCountingSink;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import java.util.Properties;

// 手动实现KafkaSerializationSchema,按照需求写入不同的topic
public class MyKafkaProduce {

    // 初始化properties
    private static Properties initProperties(){
        Properties Props = new Properties();
        Props.setProperty("bootstrap.servers", "learn:9092");
        return Props;
    }


    // 获取FlinkKafkaProducer
    public static FlinkKafkaProducer<String> getKafkaConsumer(){
        Properties properties = MyKafkaProduce.initProperties();
        FlinkKafkaProducer<String> finkKafkaProducer = new FlinkKafkaProducer<String>("", new MyKafkaSerializationSchema(),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return finkKafkaProducer;
    }



    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // env.getConfig().setLatencyTrackingInterval(2000);


        env.setParallelism(1);



        StreamSource streamSource = new StreamSource(new SourceFunction() {
            @Override
            public void run(SourceContext sourceContext) throws Exception {
            }

            @Override
            public void cancel() {

            }
        });

        StreamSink streamSink=new StreamSink(new SinkFunction() {
            @Override
            public void invoke(Object value, Context context) throws Exception {

            }
        });



        //界面上指定参数(本地测试可以忽略，设置了默认值)
        final ParameterTool params = ParameterTool.fromArgs(args);
        String hostName = params.get("hostname", "localhost");
        int port = params.getInt("port", 9000);

        //数据来源(获取sourceStream)
        DataStreamSource<String> sourceStream = env.socketTextStream(hostName, port, "\n");

        sourceStream.print();

        FlinkKafkaProducer<String> kafkaProducer = MyKafkaProduce.getKafkaConsumer();

        sourceStream.addSink(kafkaProducer).name("kafkaSink");

        env.execute("kafkaSerializationTest");
    }


}
