package com.bigdata.dis.sdk.demo.data;

import com.bigdata.dis.data.iface.request.PutRecordsRequest;
import com.bigdata.dis.data.iface.request.PutRecordsRequestEntry;
import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Public;
import org.apache.commons.lang3.RandomStringUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RandomData implements IData {

    private List<ByteBuffer> datas = new ArrayList<>(Constants.RECORD_IN_REQUEST);

    public RandomData() {
        for (int i = 0; i < Constants.RECORD_IN_REQUEST; i++) {
            datas.add(ByteBuffer.wrap(RandomStringUtils.randomAlphanumeric(Constants.RECORD_LENGTH).getBytes()));
        }
    }

    @Override
    public PutRecordsRequest createRequest() {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(Constants.STREAM_NAME);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>(Constants.RECORD_IN_REQUEST);
        for (int i = 0; i < Constants.RECORD_IN_REQUEST; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(datas.get(i));
            putRecordsRequestEntry.setPartitionKey(Public.randomKey());
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }
        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        return putRecordsRequest;
    }
}
