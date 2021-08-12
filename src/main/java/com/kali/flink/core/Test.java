package com.kali.flink.core;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> stream = env.fromElements("aa", "bb");
        List<String> list=new ArrayList<>();
        list.add("a");
        list.add("b");
        DataSource<String> dataSource = env.fromCollection(list);
        dataSource.print();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

            }
        };
        Thread thread = new Thread(runnable);
        thread.start();
        env.execute();
    }
}
