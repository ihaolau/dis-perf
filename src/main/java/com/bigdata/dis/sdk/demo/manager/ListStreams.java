package com.bigdata.dis.sdk.demo.manager;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.DISClient;
import com.huaweicloud.dis.iface.stream.request.ListStreamsRequest;
import com.huaweicloud.dis.iface.stream.response.ListStreamsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ListStreams {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListStreams.class);

    public static void main(String[] args) {
        new ListStreams().run();
    }

    public List<String> run() {
        DISClient disClient = new DISClient(Constants.DIS_CONFIG);

        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        listStreamsRequest.setLimit(100);

        String startStreamName = null;
        ListStreamsResult listStreamsResult = null;
        List<String> streams = new ArrayList<>();
        long start = System.currentTimeMillis();
        do {
            listStreamsRequest.setExclusivetartStreamName(startStreamName);
            listStreamsResult = disClient.listStreams(listStreamsRequest);
            streams.addAll(listStreamsResult.getStreamNames());
            if (streams.size() > 0) {
                startStreamName = streams.get(streams.size() - 1);
            }
        }
        while (listStreamsResult.getHasMoreStreams());

        for (int i = 1; i <= streams.size(); i++) {
            LOGGER.info("{}\t\t {}", i, streams.get(i - 1));
        }
        LOGGER.info("Success to list {} streams, cost {}ms", streams.size(), (System.currentTimeMillis() - start));
        return streams;
    }
}
