package com.bigdata.dis.sdk.demo.flink;


import com.huaweicloud.dis.DIS;
import com.huaweicloud.dis.DISClient;
import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.exception.DISClientException;
import com.huaweicloud.dis.iface.data.request.GetCheckpointRequest;
import com.huaweicloud.dis.iface.data.request.GetPartitionCursorRequest;
import com.huaweicloud.dis.iface.data.request.GetRecordsRequest;
import com.huaweicloud.dis.iface.data.response.GetCheckpointResult;
import com.huaweicloud.dis.iface.data.response.GetPartitionCursorResult;
import com.huaweicloud.dis.iface.data.response.GetRecordsResult;
import com.huaweicloud.dis.iface.stream.request.DescribeStreamRequest;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import com.huaweicloud.dis.iface.stream.response.PartitionResult;
import com.huaweicloud.dis.util.CheckpointTypeEnum;
import com.huaweicloud.dis.util.PartitionCursorTypeEnum;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DISSource extends RichParallelSourceFunction<PartitionRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DISSource.class);

    public static String CONFIG_APP_NAME = "appName";
    public static String CONFIG_AUTO_RESET_OFFSET = "auto.reset.offset";
    private String streamName = null;
    private DIS dis = null;
    private int partitionNum;
    private Map<String, String> disParams;
    private volatile boolean isRunning = true;

    public DISSource(String streamName, Map<String, String> disParams) {
        this.streamName = streamName;
        // contains  ak, sk, projectId, endpoint, region, appName
        this.disParams = disParams;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        DISConfig disConfig = new DISConfig();
        disConfig.putAll(this.disParams);
        this.dis = new DISClient(disConfig);
        this.partitionNum = getPartitions(streamName).size();
    }

    /**
     * 产生数据
     */
    @Override
    public void run(SourceContext<PartitionRecord> sourceContext) throws Exception {
        // task num
        int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
        // total task
        int taskPara = getRuntimeContext().getNumberOfParallelSubtasks();

        Map<String, String> cursorsMap = new ConcurrentHashMap<>();
        for (int i = 0; i < partitionNum; i++) {
            if (i % taskPara == taskIdx) {
                String cursor = getCursor(streamName, String.valueOf(i));
                LOGGER.info("Partition {}, cursor {}.", i, cursor);
                if (cursor != null) {
                    cursorsMap.put(String.valueOf(i), cursor);
                }
            }
        }

        GetRecordsRequest recordsRequest = new GetRecordsRequest();
        recordsRequest.setLimit(10000);
        GetRecordsResult recordResponse;

        while (isRunning && cursorsMap.size() > 0) {
            Iterator<String> iterator = cursorsMap.keySet().iterator();
            while (isRunning && iterator.hasNext()) {
                String partition = iterator.next();
                String cursor = cursorsMap.get(partition);
                long start = System.currentTimeMillis();
                //LOGGER.error("Start to getRecord {}, cursor {}", partition, cursor);
                recordsRequest.setPartitionCursor(cursor);
                try {
                    recordResponse = dis.getRecords(recordsRequest);
                    if (recordResponse.getRecords().size() > 0) {
                        LOGGER.info("End to getRecord, partition {}, size {}, cost {}ms", partition, recordResponse.getRecords().size(), (System.currentTimeMillis() - start));
                        sourceContext.collect(new PartitionRecord(streamName, partition, recordResponse.getRecords()));
                    }
                    String nextCursor = recordResponse.getNextPartitionCursor();
                    if (nextCursor != null) {
                        cursorsMap.put(partition, nextCursor);
                    } else {
                        cursorsMap.remove(partition);
                    }
                } catch (DISClientException e) {
                    String msg = e.getMessage();
                    if (msg.contains("Partition is expired")) {
                        //{"errorCode":"DIS.4319","message":"Partition is expired. [stream][a6134732688d47209577138477d86bda][shardId-0000000001]"}
                        cursorsMap.remove(partition);
                    } else {
                        throw e;
                    }
                } catch (Exception e) {
                    LOGGER.error("Error to getRecord, partition " + partition +
                            ", cost " + (System.currentTimeMillis() - start) + ", cursor " + cursor, e);
                    String newCursor = getCursor(streamName, partition);
                    cursorsMap.put(partition, newCursor);
                    Thread.sleep(1000);
                }
            }
        }

    }

    @Override
    public void cancel() {
        LOGGER.info("DISSource cancel...");
        isRunning = false;
    }

    private String getCursor(String streamName, String partition) {
        String cursor = null;
        if (disParams.get(CONFIG_APP_NAME) != null) {
            try {
                GetCheckpointResult getCheckpointResult = getCheckpoint(streamName, partition, disParams.get(CONFIG_APP_NAME));
                String sequenceNumber = getCheckpointResult.getSequenceNumber();
                if (!"-1".equalsIgnoreCase(sequenceNumber)) {
                    LOGGER.info("Partition {} starts after checkpoint, sequenceNumber {}", partition, getCheckpointResult.getSequenceNumber());
                    cursor = getCursor(streamName, partition, PartitionCursorTypeEnum.AFTER_SEQUENCE_NUMBER, Long.valueOf(sequenceNumber));
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }

        if (cursor == null) {
            if ("earliest".equalsIgnoreCase(disParams.get(CONFIG_AUTO_RESET_OFFSET))) {
                LOGGER.info("Partition {} starts with earliest", partition);
                cursor = getCursor(streamName, partition, PartitionCursorTypeEnum.TRIM_HORIZON, null);
            } else {
                LOGGER.info("Partition {} starts with latest", partition);
                cursor = getCursor(streamName, partition, PartitionCursorTypeEnum.LATEST, null);
            }
        }
        return cursor;
    }

    private String getCursor(String streamName, String partition, PartitionCursorTypeEnum cursorTypeEnum, Object value) {
        GetPartitionCursorRequest request = new GetPartitionCursorRequest();
        request.setStreamName(streamName);
        request.setPartitionId(partition);
        request.setCursorType(cursorTypeEnum.name());
        if (cursorTypeEnum == PartitionCursorTypeEnum.AT_SEQUENCE_NUMBER || cursorTypeEnum == PartitionCursorTypeEnum.AFTER_SEQUENCE_NUMBER) {
            request.setStartingSequenceNumber(String.valueOf(value));
        } else if (cursorTypeEnum == PartitionCursorTypeEnum.AT_TIMESTAMP) {
            request.setTimestamp((Long) value);
        }

        GetPartitionCursorResult partitionCursor = null;
        try {
            partitionCursor = dis.getPartitionCursor(request);
        } catch (DISClientException e) {
            String msg = e.getMessage();
            if (msg.contains("timestamp is expired")) {
                // {"errorCode":"DIS.4300","message":"Request error. [timestamp is expired, timestamp should be larger than 1536237912897]"}
                Matcher matcher = Pattern.compile("\\d{13}").matcher(msg);
                if (matcher.find()) {
                    String newValue = matcher.group(0);
                    return getCursor(streamName, partition, cursorTypeEnum, newValue);
                }
            } else if (msg.contains("Partition is expired")) {
                //{"errorCode":"DIS.4319","message":"Partition is expired. [stream][a6134732688d47209577138477d86bda][shardId-0000000001]"}
                return null;
            }
            throw e;
        }
        return partitionCursor.getPartitionCursor();
    }

    private List<PartitionResult> getPartitions(String streamName) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
        describeStreamRequest.setLimitPartitions(100);
        List<PartitionResult> partitions = new ArrayList<>();
        DescribeStreamResult describeStreamResult;
        String startPartition = "";
        do {
            describeStreamRequest.setStartPartitionId(startPartition);
            describeStreamResult = dis.describeStream(describeStreamRequest);
            startPartition = describeStreamResult.getPartitions().get(describeStreamResult.getPartitions().size() - 1).getPartitionId();
            for (int i = 0; i < describeStreamResult.getPartitions().size(); i++) {
                if ("ACTIVE".equals(describeStreamResult.getPartitions().get(i).getStatus())) {
                    partitions.add(describeStreamResult.getPartitions().get(i));
                }
            }
        }
        while (describeStreamResult.getHasMorePartitions());
        return partitions;
    }

    private GetCheckpointResult getCheckpoint(String streamName, String partition, String appName) {
        GetCheckpointRequest getCheckpointRequest = new GetCheckpointRequest();
        getCheckpointRequest.setStreamName(streamName);
        getCheckpointRequest.setAppName(appName);
        getCheckpointRequest.setCheckpointType(CheckpointTypeEnum.LAST_READ.name());
        getCheckpointRequest.setPartitionId(partition);
        try {
            return dis.getCheckpoint(getCheckpointRequest);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("DIS.4332")) {
                try {
                    dis.createApp(appName);
                    LOGGER.warn("App {} not exist and create successful.", appName);
                } catch (Exception e2) {
                    if (e.getMessage() == null || !e.getMessage().contains("DIS.4330")) {
                        LOGGER.error("App {} not exist and create failed.", appName);
                        throw e;
                    }
                }
                return getCheckpoint(streamName, partition, appName);
            }
            throw e;
        }
    }
}


