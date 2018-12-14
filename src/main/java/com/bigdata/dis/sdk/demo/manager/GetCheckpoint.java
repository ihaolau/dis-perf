package com.bigdata.dis.sdk.demo.manager;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.DIS;
import com.huaweicloud.dis.iface.data.request.GetCheckpointRequest;
import com.huaweicloud.dis.iface.data.response.GetCheckpointResult;
import com.huaweicloud.dis.util.CheckpointTypeEnum;
import com.huaweicloud.dis.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetCheckpoint {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetCheckpoint.class);

    public static void main(String[] args) {
        DIS dic = DISUtil.getInstance();
        String streamName = DISUtil.getStreamName();
        String appName = DISUtil.getAppName();

        GetCheckpointRequest getCheckpointRequest = new GetCheckpointRequest();
        getCheckpointRequest.setStreamName(streamName);
        getCheckpointRequest.setAppName(appName);
        getCheckpointRequest.setCheckpointType(CheckpointTypeEnum.LAST_READ.name());
        getCheckpointRequest.setPartitionId(String.valueOf(Constants.CHECKPOINT_PARTITION_ID));

        long start = System.currentTimeMillis();
        try {
            GetCheckpointResult getCheckpointResult = dic.getCheckpoint(getCheckpointRequest);
            LOGGER.info("Success to getCheckpoint [{}], request[{}], cost {}ms",
                    getCheckpointResult.getSequenceNumber(), JsonUtils.objToJson(getCheckpointRequest), System.currentTimeMillis() - start);
        } catch (Exception e) {
            LOGGER.error("Failed to getCheckpoint [{}]", JsonUtils.objToJson(getCheckpointRequest), e);
        }
    }
}
