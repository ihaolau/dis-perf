package com.bigdata.dis.sdk.demo.manager;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Public;
import com.huaweicloud.dis.DISClient;
import com.huaweicloud.dis.iface.stream.request.ListStreamsRequest;
import com.huaweicloud.dis.iface.stream.response.ListStreamsResult;
import com.huaweicloud.dis.iface.stream.response.StreamInfo;
import com.huaweicloud.dis.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ListStreams {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListStreams.class);

    public static void main(String[] args) {
        new ListStreams().run();
    }

    public List<StreamInfo> run() {
        DISClient disClient = new DISClient(Constants.DIS_CONFIG);

        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        listStreamsRequest.setLimit(100);

        String startStreamName = null;
        ListStreamsResult listStreamsResult = null;
        List<StreamInfo> streamInfos = new ArrayList<>();
        long start = System.currentTimeMillis();
        do {
            listStreamsRequest.setExclusivetartStreamName(startStreamName);
            listStreamsResult = disClient.listStreams(listStreamsRequest);
            streamInfos.addAll(listStreamsResult.getStreamInfos());
            if (streamInfos.size() > 0) {
                startStreamName = streamInfos.get(streamInfos.size() - 1).getStreamName();
            }
        }
        while (listStreamsResult.getHasMoreStreams());

        if (streamInfos.size() > 0) {
            streamInfos.sort(new Comparator<StreamInfo>() {
                @Override
                public int compare(StreamInfo o1, StreamInfo o2) {
                    return Long.compare(o1.getCreateTime(), o2.getCreateTime());
                }
            });
        }
        for (int i = 1; i <= streamInfos.size(); i++) {
            StreamInfo streamInfo = streamInfos.get(i - 1);
            LOGGER.info("{}\t\t {} [{}]", i, JsonUtils.objToJson(streamInfo), Public.formatTimestamp(streamInfo.getCreateTime()));
        }
        LOGGER.info("Success to list {} streams, cost {}ms", streamInfos.size(), (System.currentTimeMillis() - start));
        return streamInfos;
    }
}
