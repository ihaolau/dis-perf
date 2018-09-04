package com.bigdata.dis.sdk.demo.manager;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.DISClient;
import com.huaweicloud.dis.iface.stream.request.DescribeStreamRequest;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import com.huaweicloud.dis.iface.stream.response.PartitionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 获取通道详情
 */
public class DescribeStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(DescribeStream.class);

    public static void main(String[] args) {
        new DescribeStream().run(Constants.STREAM_NAME);
    }

    public List<PartitionResult> run(String streamName) {
        DISClient disClient = new DISClient(Constants.DIS_CONFIG);

        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
        describeStreamRequest.setLimitPartitions(100);
        List<PartitionResult> partitions = new ArrayList<>();
        DescribeStreamResult describeStreamResult;
        String startPartition = "";
        long start = System.currentTimeMillis();
        do {
            describeStreamRequest.setStartPartitionId(startPartition);
            describeStreamResult = disClient.describeStream(describeStreamRequest);
            partitions.addAll(describeStreamResult.getPartitions());
            startPartition = partitions.get(partitions.size() - 1).getPartitionId();
        }
        while (describeStreamResult.getHasMorePartitions());

        long total = 0;
        LOGGER.info("StreamType {}, RetentionPeriod {}.", describeStreamResult.getStreamType(), describeStreamResult.getRetentionPeriod());
        for (PartitionResult partition : partitions) {
            String last = partition.getSequenceNumberRange().split(":")[1].trim();
            total += Long.valueOf(last.substring(0, last.length() - 1));
            LOGGER.info("PartitionId='{}', SequenceNumberRange='{}', Status='{}', HashRange='{}'",
                    partition.getPartitionId(), partition.getSequenceNumberRange(),
                    partition.getStatus(), partition.getHashRange());
        }
        LOGGER.info("Success to describe stream {}, total records {}, cost {}ms",
                describeStreamRequest.getStreamName(), total, (System.currentTimeMillis() - start));
        return partitions;
    }
}
