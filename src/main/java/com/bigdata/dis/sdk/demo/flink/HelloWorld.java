package com.bigdata.dis.sdk.demo.flink;

import com.huaweicloud.dis.iface.data.response.Record;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

public class HelloWorld {
    public static void main(String[] args) throws Exception {

        String streamName = args[0];
        int partitionNum = Integer.valueOf(args[1]);
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        // env.setParallelism(4);//并发度  
        //DataStream<String> dataStream = env
        //        .readTextFile("D:/flinkdata/helloworld"); // 1:(flink storm
        // )(hadoop hive)
        DataStream<List<Record>> dataStream = env.addSource(new DISSource(streamName, partitionNum));
        dataStream
                .flatMap(
                        new FlatMapFunction<List<Record>, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(List<Record> input,
                                                Collector<Tuple2<String, Integer>> collector)
                                    throws Exception {
                                for (Record obj : input) {
                                    collector.collect(new Tuple2<String, Integer>(
                                            new String(obj.getData().array()), 1));// (这里很关键，表示0位置是word，1的位置是1次数)
                                }
                            }
                        })// 2:(flink 1)(storm 1)  
                .keyBy(0)// 3:以第0个位置的值，做分区。
                .sum(1)// (flink:8)(storm:5)，对第1个位置的值做sum的操作。  
                .printToErr();
        env.execute();// 启动任务
    }

}  