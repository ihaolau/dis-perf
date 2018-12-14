package com.bigdata.dis.sdk.demo.manager;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.DIS;
import com.huaweicloud.dis.iface.data.request.DeleteCheckpointRequest;
import com.huaweicloud.dis.util.CheckpointTypeEnum;
import com.huaweicloud.dis.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delete Checkpoint Example
 */
public class DeleteCheckpoint {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteCheckpoint.class);

    public static void main(String[] args) {
        DIS dic = DISUtil.getInstance();
        String streamName = DISUtil.getStreamName();
        String appName = DISUtil.getAppName();

        DeleteCheckpointRequest deleteCheckpointRequest = new DeleteCheckpointRequest();
        deleteCheckpointRequest.setCheckpointType(CheckpointTypeEnum.LAST_READ.name());
        deleteCheckpointRequest.setStreamName(streamName);
        deleteCheckpointRequest.setAppName(appName);
        deleteCheckpointRequest.setPartitionId(String.valueOf(Constants.CHECKPOINT_PARTITION_ID));

        long start = System.currentTimeMillis();
        try {
            dic.deleteCheckpoint(deleteCheckpointRequest);
            LOGGER.info("Success to deleteCheckpoint [{}], cost {}ms",
                    JsonUtils.objToJson(deleteCheckpointRequest), (System.currentTimeMillis() - start));
        } catch (Exception e) {
            LOGGER.error("Failed to deleteCheckpoint [{}]", JsonUtils.objToJson(deleteCheckpointRequest), e);
        }
    }
}
