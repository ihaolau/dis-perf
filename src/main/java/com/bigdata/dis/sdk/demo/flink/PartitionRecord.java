package com.bigdata.dis.sdk.demo.flink;

import com.huaweicloud.dis.iface.data.response.Record;

import java.util.List;

public class PartitionRecord {
    private String streamName;

    private String partition;

    private List<Record> records;

    public PartitionRecord(String streamName, String partition, List<Record> records) {
        this.streamName = streamName;
        this.partition = partition;
        this.records = records;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public List<Record> getRecords() {
        return records;
    }

    public void setRecords(List<Record> records) {
        this.records = records;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }
}