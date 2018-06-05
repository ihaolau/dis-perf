package com.bigdata.dis.sdk.demo.data;

import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;

public interface IData {
    PutRecordsRequest createRequest(String streamName);
}
