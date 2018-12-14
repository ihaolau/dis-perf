package com.bigdata.dis.sdk.demo.flink;

import com.huaweicloud.dis.DIS;
import com.huaweicloud.dis.DISClient;
import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.iface.data.request.CommitCheckpointRequest;
import com.huaweicloud.dis.iface.data.response.Record;
import com.huaweicloud.dis.util.CheckpointTypeEnum;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConsoleSink extends RichSinkFunction<PartitionRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsoleSink.class);

    private Queue<List<String>> q = new ConcurrentLinkedQueue<List<String>>();
    private String streamName;
    private Map<String, String> disParams;
    private DIS dis = null;

    public ConsoleSink(String streamName, Map<String, String> disParams) {
        this.streamName = streamName;
        this.disParams = disParams;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        DISConfig disConfig = new DISConfig();
        disConfig.putAll(this.disParams);
        this.dis = new DISClient(disConfig);
        LOGGER.info("Start Console Sink.");
    }

    @Override
    public void invoke(PartitionRecord values) throws Exception {
        for (Record record : values.getRecords()) {
            LOGGER.info("Get [{}], partition {}, sequenceNumber {}", new String(record.getData().array()), values.getPartition(), record.getSequenceNumber());
        }
        if (disParams.get(DISSource.CONFIG_APP_NAME) != null && values.getRecords().size() > 0) {
            CommitCheckpointRequest commitCheckpointRequest = new CommitCheckpointRequest();
            commitCheckpointRequest.setCheckpointType(CheckpointTypeEnum.LAST_READ.name());
            commitCheckpointRequest.setStreamName(streamName);
            commitCheckpointRequest.setAppName(disParams.get(DISSource.CONFIG_APP_NAME));
            // 需要提交的sequenceNumber
            commitCheckpointRequest.setSequenceNumber(
                    Long.valueOf(values.getRecords().get(values.getRecords().size() - 1).getSequenceNumber()) + 1L + "");
            // 分区编号
            commitCheckpointRequest.setPartitionId(values.getPartition());

            try {
                dis.commitCheckpoint(commitCheckpointRequest);
                LOGGER.info("Success to commitCheckpoint, {}", commitCheckpointRequest.toString());
            } catch (Exception e) {
                LOGGER.error("Failed to commitCheckpoint {}", commitCheckpointRequest.toString(), e);
            }
        }
    }
}
