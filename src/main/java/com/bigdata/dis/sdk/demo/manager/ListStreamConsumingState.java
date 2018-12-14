package com.bigdata.dis.sdk.demo.manager;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.DISClient;
import com.huaweicloud.dis.iface.app.request.ListStreamConsumingStateRequest;
import com.huaweicloud.dis.iface.app.response.ListStreamConsumingStateResult;
import com.huaweicloud.dis.iface.app.response.PartitionConsumingState;
import com.huaweicloud.dis.util.CheckpointTypeEnum;
import com.huaweicloud.dis.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ListStreamConsumingState {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListStreamConsumingState.class);

    public static void main(String[] args) {
        new ListStreamConsumingState().run(Constants.STREAM_NAME);
    }

    public ListStreamConsumingStateResult run(String streamName) {
        DISClient disClient = new DISClient(Constants.DIS_CONFIG);

        ListStreamConsumingStateRequest request = new ListStreamConsumingStateRequest();
        request.setAppName(Constants.CHECKPOINT_APP_NAME);
        request.setCheckpointType(CheckpointTypeEnum.LAST_READ.name());
        request.setStreamName(streamName);
        request.setLimit(100);
        String startPartitionId = "0";

        ListStreamConsumingStateResult response = null;
        List<PartitionConsumingState> partitionConsumingStates = new ArrayList<>();
        long start = System.currentTimeMillis();
        do {
            request.setStartPartitionId(startPartitionId);
            response = disClient.listStreamConsumingState(request);
            partitionConsumingStates.addAll(response.getPartitionConsumingStates());
            if (partitionConsumingStates.size() > 0) {
                startPartitionId =
                        Integer.valueOf(partitionConsumingStates.get(partitionConsumingStates.size() - 1).getPartitionId()) + 1 + "";
            }
        }
        while (response.getHasMore());

        for (int i = 1; i <= partitionConsumingStates.size(); i++) {
            PartitionConsumingState partitionConsumingState = partitionConsumingStates.get(i - 1);
            LOGGER.info("{}\t\t {}", i, JsonUtils.objToJson(partitionConsumingState));
        }
        LOGGER.info("Success to listStreamConsumingState [{}], cost {}ms", streamName, (System.currentTimeMillis() - start));
        return response;
    }
}
