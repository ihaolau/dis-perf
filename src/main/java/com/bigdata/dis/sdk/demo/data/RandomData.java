package com.bigdata.dis.sdk.demo.data;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Public;
import com.huaweicloud.dis.iface.data.iface.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.iface.request.PutRecordsRequestEntry;
import org.apache.commons.lang3.RandomStringUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RandomData implements IData {

    private List<ByteBuffer> datas = new ArrayList<>(Constants.PRODUCER_REQUEST_RECORD_NUM);

    public RandomData() {
        for (int i = 0; i < Constants.PRODUCER_REQUEST_RECORD_NUM; i++) {
            datas.add(ByteBuffer.wrap(RandomStringUtils.randomAlphanumeric(Constants.PRODUCER_RECORD_LENGTH).getBytes()));
        }
    }

    @Override
    public PutRecordsRequest createRequest(String streamName) {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>(Constants.PRODUCER_REQUEST_RECORD_NUM);
        for (int i = 0; i < Constants.PRODUCER_REQUEST_RECORD_NUM; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(datas.get(i));
            putRecordsRequestEntry.setPartitionKey(Public.randomKey());
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }
        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        return putRecordsRequest;
    }
}
