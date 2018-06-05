package com.bigdata.dis.sdk.demo.manager;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.DISClient;
import com.huaweicloud.dis.iface.stream.request.DeleteStreamRequest;
import com.huaweicloud.dis.iface.stream.response.DeleteStreamResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DeleteStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteStream.class);

    public static void main(String[] args) {
        new DeleteStream().run(Constants.STREAM_NAME);
    }

    public DeleteStreamResult run(String streamName) {
        DISClient disClient = new DISClient(Constants.DIS_CONFIG);

        DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
        deleteStreamRequest.setStreamName(streamName);
        long start = System.currentTimeMillis();
        DeleteStreamResult deleteStreamResult = null;
        try {
            deleteStreamResult = disClient.deleteStream(deleteStreamRequest);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }

        LOGGER.info("Success to delete stream {}, cost {}ms",
                deleteStreamRequest.getStreamName(), (System.currentTimeMillis() - start));
        return deleteStreamResult;
    }

    public void runAll() {
        List<String> streams = new ListStreams().run();
        if (streams == null || streams.size() == 0) {
            LOGGER.info("No stream.");
        }

        for (String stream : streams) {
            new DeleteStream().run(stream);
        }
    }
}
