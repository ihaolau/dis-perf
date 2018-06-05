package com.bigdata.dis.sdk.demo.data.custom.hr;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Public;
import com.bigdata.dis.sdk.demo.data.IData;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import org.apache.commons.lang3.RandomStringUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class HRData implements IData {

    private List<ByteBuffer> datas = new ArrayList<>(Constants.PRODUCER_REQUEST_RECORD_NUM);

    public HRData() {
        for (int i = 0; i < Constants.PRODUCER_REQUEST_RECORD_NUM; i++) {
            datas.add(ByteBuffer.wrap(RandomStringUtils.randomAlphanumeric(Constants.PRODUCER_RECORD_LENGTH).getBytes()));
        }
    }

    @Override
    public PutRecordsRequest createRequest(String streamName) {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        for (EquInfo equInfo : EquInfo.getRandom()) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(ByteBuffer.wrap(equInfo.toString().getBytes()));
            putRecordsRequestEntry.setPartitionKey(Public.randomKey());
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }
        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        return putRecordsRequest;
    }
}
