package com.kali.flink.core.connector.generator;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;


import java.io.File;
import java.time.Duration;
import java.util.Random;

public class GeneratorSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);

        DataStreamSource<String> dataStreamSource = new GeneratorSource().dataStreamSource(env);

//        dataStreamSource.print("print");
        dataStreamSource.sinkTo(FileSink
                .forRowFormat(Path.fromLocalFile(new File("./logs")), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(15))
                                .withInactivityInterval(Duration.ofMinutes(5))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                                .build())
                .build());
//        dataStreamSource.writeAsText("./user.log");
        env.execute("generator");
    }

    public DataStreamSource<String> dataStreamSource(StreamExecutionEnvironment env) {
        return env.fromSource(new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public void open(SourceReaderContext readerContext) throws Exception {
                GeneratorFunction.super.open(readerContext);
            }

            @Override
            public void close() throws Exception {
                GeneratorFunction.super.close();
            }

            @Override
            public String map(Long value) throws Exception {
                Thread.sleep(1000);
                return JSON.toJSONString(new User(String.valueOf(new Random().nextInt(5)), String.valueOf(new Random().nextInt(3)), System.currentTimeMillis()));
            }

        }, 100000000, Types.STRING), WatermarkStrategy.noWatermarks(), "data-generator");
    }

}


