package com.bigdata.dis.sdk.demo.flink;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.iface.data.response.Record;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DISFlinkTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DISFlinkTest.class);

    public static void main(String[] args) throws Exception {

        Map<String, String> disParams = new HashMap(Constants.DIS_CONFIG);
        // 通道名
        String streamName = args[0];
        // 并行度
        int envP = Integer.valueOf(args[1]);

        int sourceP = envP;
        if (args.length > 2) {
            // Source并行度
            sourceP = Integer.valueOf(args[2]);
        }
        // Sink并行度
        int sinkP = envP;
        if (args.length > 3) {
            // Source并行度
            sinkP = Integer.valueOf(args[3]);
        }

        if (args.length > 4) {
            // App名称
            disParams.put(DISSource.CONFIG_APP_NAME, args[4]);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(envP);
        DataStream<PartitionRecord> dataStream = env.addSource(new DISSource(streamName, disParams))
                .setParallelism(sourceP).disableChaining();
        //.flatMap(new DisMap());

        dataStream.addSink(new ConsoleSink(streamName, disParams)).setParallelism(sinkP);
//        dataStream
//                .flatMap(
//                        new FlatMapFunction<List<Record>, Tuple2<String, Integer>>() {
//                            @Override
//                            public void flatMap(List<Record> input,
//                                                Collector<Tuple2<String, Integer>> collector)
//                                    throws Exception {
//                                for (Record obj : input) {
//                                    collector.collect(new Tuple2<String, Integer>(
//                                            new String(obj.getData().array()), 1));// (这里很关键，表示0位置是word，1的位置是1次数)
//                                }
//                            }
//                        })// 2:(flink 1)(storm 1)
//                .keyBy(0)// 3:以第0个位置的值，做分区。
//                .sum(1)// (flink:8)(storm:5)，对第1个位置的值做sum的操作。
//                .printToErr();
        env.execute();// 启动任务
    }

    static class DisMap implements FlatMapFunction<PartitionRecord, List<String>> {
        @Override
        public void flatMap(PartitionRecord o, Collector<List<String>> collector) throws Exception {
            for (Record r : o.getRecords()) {
                collector.collect(Arrays.asList(new String(r.getData().array()).split(" ")));
            }
        }
    }
}