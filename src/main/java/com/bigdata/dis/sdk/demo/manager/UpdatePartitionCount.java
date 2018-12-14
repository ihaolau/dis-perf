package com.bigdata.dis.sdk.demo.manager;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.DIS;
import com.huaweicloud.dis.iface.stream.request.UpdatePartitionCountRequest;
import com.huaweicloud.dis.iface.stream.response.UpdatePartitionCountResult;
import com.huaweicloud.dis.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Update Partition Count Example
 */
public class UpdatePartitionCount {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdatePartitionCount.class);

    public static void main(String[] args) {
        DIS dic = DISUtil.getInstance();
        String streamName = DISUtil.getStreamName();
        // 目标分区数量
        int targetPartitionCount = Constants.UPDATE_PARTITION_COUNT;

        UpdatePartitionCountRequest update = new UpdatePartitionCountRequest();
        update.setStreamName(streamName);
        update.setTargetPartitionCount(targetPartitionCount);
        try {
            UpdatePartitionCountResult updatePartitionCountResult = dic.updatePartitionCount(update);
            LOGGER.info("Success to update partition count [{}]", JsonUtils.objToJson(updatePartitionCountResult));
        } catch (Exception e) {
            LOGGER.error("Failed to update partition count [{}]", JsonUtils.objToJson(update), e);
        }
    }
}
