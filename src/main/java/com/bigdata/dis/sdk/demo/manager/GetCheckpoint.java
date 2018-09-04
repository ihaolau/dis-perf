package com.bigdata.dis.sdk.demo.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaweicloud.dis.DIS;
import com.huaweicloud.dis.iface.data.request.GetCheckpointRequest;
import com.huaweicloud.dis.iface.data.response.GetCheckpointResult;
import com.huaweicloud.dis.util.CheckpointTypeEnum;

/**
 * Commit Checkpoint Example
 */
public class GetCheckpoint
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GetCheckpoint.class);
    
    public static void main(String[] args)
    {
        DIS dic = DISUtil.getInstance();
        String streamName = DISUtil.getStreamName();
        String appName = DISUtil.getAppName();
        
        GetCheckpointRequest getCheckpointRequest = new GetCheckpointRequest();
        getCheckpointRequest.setStreamName(streamName);
        getCheckpointRequest.setAppId(appName);
        getCheckpointRequest.setCheckpointType(CheckpointTypeEnum.LAST_READ.name());
        getCheckpointRequest.setPartitionId("0");
        
        try
        {
            GetCheckpointResult getCheckpointResult = dic.getCheckpoint(getCheckpointRequest);
            LOGGER.info("Success to getCheckpoint, {}", getCheckpointResult);
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to getCheckpoint", e);
        }
    }
}
