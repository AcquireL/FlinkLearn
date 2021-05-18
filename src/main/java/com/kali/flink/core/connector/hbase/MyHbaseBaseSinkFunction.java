package com.kali.flink.core.connector.hbase;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.hbase.sink.HBaseSinkFunction;
import org.apache.flink.connector.hbase.sink.LegacyMutationConverter;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;;


public class MyHbaseBaseSinkFunction {

    // 获取HbaseSinkFunction
    public static HBaseSinkFunction<Tuple2<Boolean, Row>> getHbaseSinkFunction(){

        String hTableName = "VDF:TEST";
        HBaseTableSchema hBaseTableSchema = new HBaseTableSchema();

        hBaseTableSchema.setRowKey("aid",String.class);
        hBaseTableSchema.addColumn("info","aid",String.class);
        hBaseTableSchema.addColumn("info","sid",String.class);
        hBaseTableSchema.addColumn("info","svid",String.class);

        Configuration hadoopConf = HBaseConfiguration.create();

        hadoopConf.set("hbase.zookeeper.quorum", "learn:2181");
        hadoopConf.set("zookeeper.znode.parent", "/hbase226");  // 默认使用的是zk的/hbase目录
        hadoopConf.set("hbase.zookeeper.property.clientPort", "2181");

        LegacyMutationConverter converter = new LegacyMutationConverter(hBaseTableSchema);

        HBaseSinkFunction<Tuple2<Boolean, Row>> hBaseSinkFunction = new HBaseSinkFunction<>(hTableName,hadoopConf,converter,64,1024,1000);

        return hBaseSinkFunction;
    }

}
