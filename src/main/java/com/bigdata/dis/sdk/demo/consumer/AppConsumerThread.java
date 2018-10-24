package com.bigdata.dis.sdk.demo.consumer;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Public;
import com.bigdata.dis.sdk.demo.common.Statistics;
import com.huaweicloud.dis.DIS;
import com.huaweicloud.dis.DISClient;
import com.huaweicloud.dis.DISClientAsync;
import com.huaweicloud.dis.DISClientAsync2;
import com.huaweicloud.dis.iface.data.request.GetPartitionCursorRequest;
import com.huaweicloud.dis.iface.data.request.GetRecordsRequest;
import com.huaweicloud.dis.iface.data.response.GetPartitionCursorResult;
import com.huaweicloud.dis.iface.data.response.GetRecordsResult;
import com.huaweicloud.dis.iface.data.response.Record;
import com.huaweicloud.dis.iface.stream.request.DescribeStreamRequest;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import com.huaweicloud.dis.iface.stream.response.PartitionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class AppConsumerThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppConsumerThread.class);

    private final DIS dis;

    private int partitionSize = 0;

    private Map<Integer, String> iteratorMap = new ConcurrentHashMap<>();

    private Statistics statistics;

    private final String streamName;

    public AppConsumerThread(String streamName, Statistics statistics) {
        this.streamName = streamName;
        this.statistics = statistics;

        if("true".equals(Constants.DIS_CONFIG.get("SyncOnAsync"))) {
        	if("true".equals(Constants.DIS_CONFIG.get("NIOAsync"))) {
        		dis = new DISClientAsync2(Constants.DIS_CONFIG);
        	}else {
        		dis = new DISClientAsync(Constants.DIS_CONFIG);
        	}
        }else {
        	dis = new DISClient(Constants.DIS_CONFIG);
        }
    }

    public void initPartition() {
        if ("auto".equals(Constants.CONSUMER_PARTITION_NUM)) {
            DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
            describeStreamRequest.setStreamName(this.streamName);
            describeStreamRequest.setLimitPartitions(100);
            DescribeStreamResult describeStreamResult = null;
            List<PartitionResult> partitions = new ArrayList<>();
            String startPartition = "0";
            do {
                describeStreamRequest.setStartPartitionId(startPartition);
                describeStreamResult = dis.describeStream(describeStreamRequest);
                partitions.addAll(describeStreamResult.getPartitions());
                startPartition = partitions.get(partitions.size() - 1).getPartitionId();
            }
            while (describeStreamResult.getHasMorePartitions());
            this.partitionSize = partitions.size();
        } else {
            this.partitionSize = Integer.valueOf(Constants.CONSUMER_PARTITION_NUM);
        }

        LOGGER.info("Stream {} has {} partitions.", this.streamName, partitionSize);
    }

    @Override
    public void run() {
        initPartition();
        try {
            CountDownLatch countDownLatch = new CountDownLatch(partitionSize);
            for (int i = 0; i < partitionSize; i++) {
                final int tmp = i;
                new Thread(new Runnable() {
                    final int partitionNum = tmp;

                    @Override
                    public void run() {
                        LOGGER.info("{}_{}_{} start.", getName(), streamName, partitionNum);
                        try {
                            GetPartitionCursorRequest request = new GetPartitionCursorRequest();
                            request.setStreamName(streamName);
                            request.setPartitionId(String.valueOf(partitionNum));
//                            request.setCursorType("AT_TIMESTAMP");
//                            long timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2018-08-02 21:46:41").getTime();
//                            request.setTimestamp(timestamp);
                            request.setCursorType(Constants.CONSUMER_CURSOR_TYPE.toString());
                            request.setStartingSequenceNumber(String.valueOf(Constants.CONSUMER_OFFSET));
                            GetPartitionCursorResult partitionCursor = dis.getPartitionCursor(request);
                            iteratorMap.put(partitionNum, partitionCursor.getPartitionCursor());

                            GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
                            getRecordsRequest.setLimit(Constants.CONSUMER_LIMIT);
                            while (true) {
                                getRecordsRequest.setPartitionCursor(iteratorMap.get(partitionNum));
                                statistics.totalRequestTimes.incrementAndGet();
                                GetRecordsResult response = null;
                                long timeStart = System.currentTimeMillis();
                                try {
                                    response = dis.getRecords(getRecordsRequest);
                                    iteratorMap.put(partitionNum, response.getNextPartitionCursor());
                                    statistics.totalRequestSuccessTimes.incrementAndGet();
                                    long cost = System.currentTimeMillis() - timeStart;
                                    statistics.totalPostponeMillis.addAndGet(cost);
                                } catch (Exception e) {
                                    LOGGER.error(e.getMessage());
                                    statistics.totalRequestFailedTimes.incrementAndGet();
                                    TimeUnit.MILLISECONDS.sleep(Constants.SERVER_FAILED_SLEEP_TIME);
                                }

                                if (response != null) {
                                    statistics.totalSendSuccessRecords.addAndGet(response.getRecords().size());
                                    if (LOGGER.isDebugEnabled()) {
                                        outputData(response, partitionNum);
                                    }
                                }

                                TimeUnit.MILLISECONDS.sleep(Constants.CONSUMER_REQUEST_SLEEP_TIME);
                            }
                        } catch (Exception e) {
                            LOGGER.error(e.getMessage(), e);
                        } finally {
                            countDownLatch.countDown();
                        }
                        LOGGER.info("{}_{}_{} stop.", getName(), streamName, partitionNum);
                    }
                }).start();
            }

            countDownLatch.await();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        }
    }

    private void outputData(GetRecordsResult getRecordsResult, int partitionNum) {
        List<Record> records = getRecordsResult.getRecords();
        if (records.isEmpty()) {
            return;
        }
        for (Record record : records) {
            String content = new String(record.getData().array());
            String sequenceNumber = record.getSequenceNumber();
            Long timestamp = record.getTimestamp();
            LOGGER.debug("Partition [{}], Content [{}], partitionKey [{}], sequenceNumber [{}], timestamp [{}]",
                    partitionNum, content, record.getPartitionKey(), sequenceNumber, Public.DATE_FORMATE.format(new Date(timestamp)));
        }
    }
}