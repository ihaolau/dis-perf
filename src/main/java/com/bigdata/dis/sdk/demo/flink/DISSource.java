package com.bigdata.dis.sdk.demo.flink;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.DIS;
import com.huaweicloud.dis.DISClient;
import com.huaweicloud.dis.iface.data.request.GetPartitionCursorRequest;
import com.huaweicloud.dis.iface.data.request.GetRecordsRequest;
import com.huaweicloud.dis.iface.data.response.GetPartitionCursorResult;
import com.huaweicloud.dis.iface.data.response.GetRecordsResult;
import com.huaweicloud.dis.iface.data.response.Record;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DISSource extends RichParallelSourceFunction<List<Record>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DISSource.class);
    int partitionNum = 16;
    String streamName = "dis-flink";

    public DISSource(String streamName, int partitionNum) {
        this.streamName = streamName;
        this.partitionNum = partitionNum;
    }

    /**
     * 产生数据
     */
    @Override
    public void run(SourceContext<List<Record>> sourceContext) throws Exception {


        int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
        int taskPara = getRuntimeContext().getNumberOfParallelSubtasks();
        DIS dis = new DISClient(Constants.DIS_CONFIG);

        Map<String, String> cursorsMap = new ConcurrentHashMap<>();
        //
        for (int i = 0; i < partitionNum; i++) {
            if (i % taskPara == taskIdx) {
                GetPartitionCursorRequest request = new GetPartitionCursorRequest();
                request.setStreamName(streamName);
                request.setPartitionId(i + "");
                request.setCursorType("LATEST");
                GetPartitionCursorResult partitionCursor = dis.getPartitionCursor(request);
                LOGGER.error(partitionCursor.getPartitionCursor());
                cursorsMap.put(String.valueOf(i), partitionCursor.getPartitionCursor());
            }
        }

        GetRecordsRequest recordsRequest = new GetRecordsRequest();

        while (true) {
            GetRecordsResult recordResponse;
            Iterator<String> iterator = cursorsMap.keySet().iterator();
            while (iterator.hasNext()) {
                recordsRequest.setLimit(10000);
                String partition = iterator.next();
                String cursor = cursorsMap.get(partition);
                long start = System.currentTimeMillis();
                //LOGGER.error("Start to getRecord {}, cursor {}", partition, cursor);
                recordsRequest.setPartitionCursor(cursor);
                try {
                    recordResponse = dis.getRecords(recordsRequest);
                    LOGGER.info("End to getRecord {}, cost {}ms, size {}", partition, (System.currentTimeMillis() - start),
                            recordResponse.getRecords().size());
                    sourceContext.collect(recordResponse.getRecords());
                    String nextCursor = recordResponse.getNextPartitionCursor();
                    cursorsMap.put(partition, nextCursor);
                } catch (Exception e) {
                    LOGGER.error("Error to getRecord, partition " + partition +
                            ", cost " + (System.currentTimeMillis() - start) + ", cursor " + cursor, e);
                }
            }
        }
    }

    /**
     * 关闭资源
     */
    @Override
    public void cancel() {
    }

}
  