package com.bigdata.dis.sdk.demo.consumer.async;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.DISAsync;
import com.huaweicloud.dis.DISClientAsync;
import com.huaweicloud.dis.core.handler.AsyncHandler;
import com.huaweicloud.dis.iface.data.iface.request.GetPartitionCursorRequest;
import com.huaweicloud.dis.iface.data.iface.request.GetRecordsRequest;
import com.huaweicloud.dis.iface.data.iface.response.GetPartitionCursorResult;
import com.huaweicloud.dis.iface.data.iface.response.GetRecordsResult;
import com.huaweicloud.dis.iface.data.iface.response.Record;
import com.huaweicloud.dis.iface.stream.iface.request.DescribeStreamRequest;
import com.huaweicloud.dis.iface.stream.iface.response.DescribeStreamResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class MulitPartitionConsumerAsyncDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(MulitPartitionConsumerAsyncDemo.class);

    static int THREAD_COUNT = 2;

    static ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

    static volatile boolean isStop = false;

    static final String STREAM_NAME = Constants.STREAM_NAME;

    public static void main(String[] args) {
        for (int m = 0; m < THREAD_COUNT; m++) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        MulitPartitionConsumerAsyncDemo.run();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            executorService.submit(th);
        }
    }

    static public void run()
            throws InterruptedException {
        final Map<Integer, Future<GetRecordsResult>> futuresMap = new HashMap<>();
        final String threadName = Thread.currentThread().getName();

        final DISAsync dis = new DISClientAsync(Constants.DIS_CONFIG, Executors.newFixedThreadPool(10));

        GetPartitionCursorRequest request = new GetPartitionCursorRequest();
        GetRecordsRequest recordsRequest = new GetRecordsRequest();
        GetPartitionCursorResult response = null;
        GetRecordsResult recordResponse = null;

        request.setStreamName(STREAM_NAME);
        request.setPartitionId("0");

        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(STREAM_NAME);
        describeStreamRequest.setStartPartitionId("");
        describeStreamRequest.setLimitPartitions(50);
        DescribeStreamResult describeStreamResult = dis.describeStream(describeStreamRequest);
        int partitionSize = describeStreamResult.getPartitions().size();
//        int partitionSize = 1;

        Map<Integer, String> iteratorMap = new HashMap<>();
        for (int i = 0; i < partitionSize; i++) {
            request.setPartitionId(i + "");
//            if (Integer.valueOf(SDKConstants.CONSUMER_SEQUENCE_NUMBER) > -1) {
//                request.setStartingSequenceNumber(SDKConstants.CONSUMER_SEQUENCE_NUMBER);
//                request.setCursorType("AT_SEQUENCE_NUMBER");
//            } else if (Integer.valueOf(SDKConstants.CONSUMER_SEQUENCE_NUMBER) == -1) {
//                request.setCursorType("LATEST");
//            } else {
//                request.setCursorType("TRIM_HORIZON");
//            }
            request.setCursorType("TRIM_HORIZON");
            response = dis.getPartitionCursor(request);
            String iterator = response.getPartitionCursor();
            iteratorMap.put(i, iterator);
        }

        Map<String, Integer> resultCount = new HashMap<>();
        String start = null;
        String end = null;
        final AtomicInteger totalCount = new AtomicInteger();

        while (!isStop) {
            for (int j = 0; j < partitionSize; j++) {
                final int flag = j;
                if (futuresMap.get(flag) == null) {
                    recordsRequest.setPartitionCursor(iteratorMap.get(flag));
                    // recordsRequest.setLimit(SDKConstants.CONSUMER_BATCH_COUNT);
//                    recordsRequest.setLimit(200);
                    try {
                        final long startMs = System.currentTimeMillis();
                        LOGGER.info("!{} Start to get Partition {}", threadName, flag);
                        futuresMap.put(flag,
                                dis.getRecordsAsync(recordsRequest, new AsyncHandler<GetRecordsResult>() {
                                    @Override
                                    public void onError(Exception exception) {
                                        String msg = exception.getMessage();
                                        if(!msg.contains("Exceeded traffic control limit"))
                                        {
                                            isStop = true;
                                        }
                                        LOGGER.error("!{} Cost " + (System.currentTimeMillis() - startMs) + "ms, " + exception.getMessage(), threadName, exception);
                                        futuresMap.remove(flag);
                                    }

                                    @Override
                                    public void onSuccess(GetRecordsResult getRecordsResult) {
                                        try {
                                            LOGGER.info("!{} Partition {} get {} records, cost {}ms, startSeq {}", threadName,
                                                    flag,
                                                    getRecordsResult.getRecords().size(), (System.currentTimeMillis() - startMs),
                                                    getRecordsResult.getRecords().get(0).getSequenceNumber());
                                            iteratorMap.put(flag, getRecordsResult.getNextPartitionCursor());
                                            if (getRecordsResult.getRecords().size() > 0) {
                                                for (Record record : getRecordsResult.getRecords()) {
                                                    totalCount.incrementAndGet();
//                                            String content =
//                                                new String(record.getData().array(), "utf-8").replaceAll("\r", "\\\\r")
//                                                    .replaceAll("\n", "\\\\n");
//                                            resultCount.merge(content, 1, (a, b) -> a + b);
//                                            LOGGER.info(
//                                                "Record[Content={}, PartitionKey={}, SequenceNumber={}], APPEND INFO[{}]",
//                                                new String(record.getData().array(), "utf-8").replaceAll("\r", "\\\\r")
//                                                    .replaceAll("\n", "\\\\n"),
//                                                record.getPartitionKey(),
//                                                record.getSequenceNumber());
                                                }
                                            } else {
                                                LOGGER.info("Response size 0");
                                                for (String s : resultCount.keySet()) {
                                                    LOGGER.info(resultCount.get(s) + " | " + s);
                                                }
                                            }
                                        } catch (Exception e) {
                                            LOGGER.error(e.getMessage());
                                        } finally {
                                            futuresMap.remove(flag);
                                        }
                                    }
                                }));
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage());
                    }

                }
//                else
                {
                    Thread.sleep(0);
                }
            }
        }
    }
}