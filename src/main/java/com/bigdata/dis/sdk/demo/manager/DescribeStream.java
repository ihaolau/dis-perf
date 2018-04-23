package com.bigdata.dis.sdk.demo.manager;

import com.bigdata.dis.sdk.DISClient;
import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.stream.iface.request.DescribeStreamRequest;
import com.bigdata.dis.stream.iface.response.DescribeStreamResult;
import com.bigdata.dis.stream.iface.response.PartitionResult;
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
        DISClient disClient = new DISClient(Constants.DIS_CONFIG);

        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(Constants.STREAM_NAME);
        describeStreamRequest.setLimitPartitions(1);
        List<PartitionResult> partitions = new ArrayList<>();
        DescribeStreamResult describeStreamResult;
        String startPartition = "0";
        do {
            describeStreamRequest.setStartPartitionId(startPartition);
            describeStreamResult = disClient.describeStream(describeStreamRequest);
            partitions.addAll(describeStreamResult.getPartitions());
            startPartition = partitions.get(partitions.size() - 1).getPartitionId();
        }
        while (describeStreamResult.getHasMorePartitions());

        for (PartitionResult partition : partitions) {
            LOGGER.info("PartitionId='{}', seqRange='{}', hashRange='{}'",
                    partition.getPartitionId(), partition.getSequenceNumberRange(), partition.getHashRange());
        }
    }
}
