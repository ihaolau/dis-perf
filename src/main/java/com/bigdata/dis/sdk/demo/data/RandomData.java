package com.bigdata.dis.sdk.demo.data;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Public;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import org.apache.commons.lang3.RandomStringUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class RandomData implements IData {

    private AtomicLong atomicLong = new AtomicLong(0);
    private List<String> datas = new ArrayList<>(Constants.PRODUCER_REQUEST_RECORD_NUM);

    public RandomData() {
        for (int i = 0; i < Constants.PRODUCER_REQUEST_RECORD_NUM; i++) {
            datas.add(" | " + RandomStringUtils.randomAlphanumeric(Constants.PRODUCER_RECORD_LENGTH));
        }
    }

    @Override
    public PutRecordsRequest createRequest(String streamName) {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>(Constants.PRODUCER_REQUEST_RECORD_NUM);
        for (int i = 0; i < Constants.PRODUCER_REQUEST_RECORD_NUM; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(ByteBuffer.wrap((atomicLong.incrementAndGet() + datas.get(i)).getBytes()));
            putRecordsRequestEntry.setPartitionKey(Public.randomKey());
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }
        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        return putRecordsRequest;
    }
}
