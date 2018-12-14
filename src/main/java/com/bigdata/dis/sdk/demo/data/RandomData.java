package com.bigdata.dis.sdk.demo.data;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Public;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import org.apache.commons.lang3.RandomStringUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class RandomData implements IData {

    private AtomicLong atomicLong = new AtomicLong(0);
    private List<String> datas = new ArrayList<>(Constants.PRODUCER_REQUEST_RECORD_NUM);
    private ThreadLocal<StringBuilder> builderThreadLocal = new ThreadLocal<StringBuilder>() {
        protected StringBuilder initialValue() {
            return new StringBuilder();
        }
    };

    public RandomData() {
        for (int i = 0; i < Constants.PRODUCER_REQUEST_RECORD_NUM; i++) {
            datas.add("|" + RandomStringUtils.randomAlphanumeric(Constants.PRODUCER_RECORD_LENGTH));
        }
    }

    public static void main(String[] args) throws InterruptedException {
        RandomData randomData = new RandomData();
        final int total = 10000;
        final int thread = 10;
        CountDownLatch countDownLatch = new CountDownLatch(thread);
        long start = System.currentTimeMillis();
        for (int i = 0; i < thread; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < 100; j++) {
                        randomData.createRequest("test");
                        randomData.createRequest1("test");
                    }

                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        System.out.println("Cost1 " + (System.currentTimeMillis() - start));

        CountDownLatch countDownLatch1 = new CountDownLatch(thread);
        long start1 = System.currentTimeMillis();
        for (int i = 0; i < thread; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < total; j++) {
                        randomData.createRequest("test");
                    }

                    countDownLatch1.countDown();
                }
            }).start();
        }
        countDownLatch1.await();
        System.out.println("Cost2 createRequest  " + (System.currentTimeMillis() - start1));


        CountDownLatch countDownLatch2 = new CountDownLatch(thread);
        long start2 = System.currentTimeMillis();
        for (int i = 0; i < thread; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < total; j++) {
                        randomData.createRequest1("test");
                    }

                    countDownLatch2.countDown();
                }
            }).start();
        }
        countDownLatch2.await();
        System.out.println("Cost3 createRequest1 " + (System.currentTimeMillis() - start2));

    }

    @Override
    public PutRecordsRequest createRequest(String streamName) {
        return createRequest(streamName, Constants.PRODUCER_REQUEST_RECORD_NUM);
    }

    public PutRecordsRequest createRequest(String streamName, int recordNum) {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>(recordNum);
        StringBuilder sb = builderThreadLocal.get();
        for (int i = 0; i < recordNum; i++) {
            sb.setLength(0);
            sb.append(atomicLong.incrementAndGet()).append("|").append(Public.currentDateTime()).append(datas.get(i));
            sb.setLength(Constants.PRODUCER_RECORD_LENGTH);
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(ByteBuffer.wrap(sb.toString().getBytes()));
            putRecordsRequestEntry.setPartitionKey(Public.randomKey());
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }
        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        return putRecordsRequest;
    }

    public PutRecordsRequest createRequest1(String streamName) {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>(Constants.PRODUCER_REQUEST_RECORD_NUM);
        for (int i = 0; i < Constants.PRODUCER_REQUEST_RECORD_NUM; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(ByteBuffer.wrap((atomicLong.incrementAndGet() + "|" + Public.currentDateTime()
                    + datas.get(i)).substring(0, Constants.PRODUCER_RECORD_LENGTH).getBytes()));
            putRecordsRequestEntry.setPartitionKey(Public.randomKey());
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }
        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        return putRecordsRequest;
    }
}
