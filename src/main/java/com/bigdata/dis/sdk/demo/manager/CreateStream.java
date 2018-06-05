package com.bigdata.dis.sdk.demo.manager;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.manager.type.DataType;
import com.cloud.sdk.util.StringUtils;
import com.huaweicloud.dis.DISClient;
import com.huaweicloud.dis.exception.DISClientException;
import com.huaweicloud.dis.iface.stream.request.CreateStreamRequest;
import com.huaweicloud.dis.iface.stream.request.OBSDestinationDescriptorRequest;
import com.huaweicloud.dis.iface.stream.response.CreateStreamResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class CreateStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateStream.class);

    public static void main(String[] args) {
        new CreateStream().run(Constants.STREAM_NAME, false);
    }

    public CreateStreamResult run(String streamName, boolean deleteIfExist) {
        DISClient disClient = new DISClient(Constants.DIS_CONFIG);

        CreateStreamRequest createStreamRequest = new CreateStreamRequest();
        createStreamRequest.setStreamName(streamName);
        createStreamRequest.setStreamType(Constants.CREATE_STREAM_TYPE);
        createStreamRequest.setDataType(DataType.BLOB.name());
        createStreamRequest.setPartitionCount(Constants.CREATE_PARTITION_NUM);
        createStreamRequest.setDataDuration(Constants.CREATE_DATA_DURATION * 24);
        if (!StringUtils.isNullOrEmpty(Constants.CREATE_OBS_BUCKET)) {
            OBSDestinationDescriptorRequest obs = new OBSDestinationDescriptorRequest();
            obs.setAgencyName(Constants.CREATE_AGENCY_NAME);
            obs.setDeliverTimeInterval(60);
            obs.setFilePrefix(streamName.replaceAll("-", "_"));
            obs.setObsBucketPath(Constants.CREATE_OBS_BUCKET);
            obs.setPartitionFormat(Constants.CREATE_PARTITION_FORMAT);
            // obs.setDeliverDataType("file_stream);
            createStreamRequest.setObsDestinationDescriptors(Collections.singletonList(obs));
        }

        long start = System.currentTimeMillis();
        CreateStreamResult createStreamResult = null;
        try {
            createStreamResult = disClient.createStream(createStreamRequest);
        } catch (DISClientException e) {
            if (e.getMessage().contains("DIS.4307") && deleteIfExist) {
                new DeleteStream().run(streamName);
                createStreamResult = disClient.createStream(createStreamRequest);
            } else {
                throw e;
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }

        LOGGER.info("Success to create stream {}, cost {}ms",
                createStreamRequest.getStreamName(), (System.currentTimeMillis() - start));

        return createStreamResult;
    }
}
