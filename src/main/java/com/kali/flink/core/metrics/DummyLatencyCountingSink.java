package com.kali.flink.core.metrics;


import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Sink that drops all data and periodically emits latency and throughput measurements
 */
public class DummyLatencyCountingSink<T> extends StreamSink<T> {
    private static final Logger logger= LoggerFactory.getLogger(DummyLatencyCountingSink.class);

    public DummyLatencyCountingSink(SinkFunction<T> sinkFunction) {
        super(sinkFunction);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        logger.warn("%{}%{}%{}%{}", "latency",
                System.currentTimeMillis() - latencyMarker.getMarkedTime(),
                latencyMarker.getSubtaskIndex(),
                getRuntimeContext().getIndexOfThisSubtask());
    }
}