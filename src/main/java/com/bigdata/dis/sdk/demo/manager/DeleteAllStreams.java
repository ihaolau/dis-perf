package com.bigdata.dis.sdk.demo.manager;

import com.huaweicloud.dis.iface.stream.response.StreamInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DeleteAllStreams extends DeleteStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteAllStreams.class);

    public static void main(String[] args) {
        new DeleteAllStreams().runAll();
    }

    public void runAll() {
        List<StreamInfo> streams = new ListStreams().run();
        if (streams == null || streams.size() == 0) {
            LOGGER.info("No stream.");
            return;
        }

        for (StreamInfo stream : streams) {
            run(stream.getStreamName());
        }
    }
}
