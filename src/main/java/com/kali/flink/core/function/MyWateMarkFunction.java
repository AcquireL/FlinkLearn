package com.kali.flink.core.function;


import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import scala.Tuple2;

public class MyWateMarkFunction implements WatermarkStrategy<Tuple2<Long,String>> {

    @Override
    public WatermarkGenerator<Tuple2<Long, String>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return null;
    }
}
