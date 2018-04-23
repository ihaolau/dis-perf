package com.bigdata.dis.sdk.demo.consumer.kafka;

import com.bigdata.dis.data.iface.request.GetPartitionCursorRequest;
import com.bigdata.dis.data.iface.request.GetRecordsRequest;
import com.bigdata.dis.data.iface.response.GetPartitionCursorResult;
import com.bigdata.dis.data.iface.response.GetRecordsResult;
import com.bigdata.dis.data.iface.response.Record;
import com.bigdata.dis.sdk.DIS;
import com.bigdata.dis.sdk.DISClient;
import com.bigdata.dis.sdk.adapter.kafka.consumer.DISKafkaConsumer;
import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Public;
import com.bigdata.dis.sdk.demo.common.Statistics;
import com.bigdata.dis.sdk.util.PartitionCursorTypeEnum;
import com.bigdata.dis.stream.iface.request.DescribeStreamRequest;
import com.bigdata.dis.stream.iface.response.DescribeStreamResult;
import com.bigdata.dis.stream.iface.response.PartitionResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class AppKafkaConsumerThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppKafkaConsumerThread.class);

    private final Consumer<String, String> consumer;

    private int partitionSize = 0;

    private Map<Integer, String> iteratorMap = new ConcurrentHashMap<>();

    private Statistics statistics;

    public AppKafkaConsumerThread(Statistics statistics) {
        this.statistics = statistics;
        consumer = new DISKafkaConsumer<String, String>(Constants.DIS_CONFIG);
    }

    public void initPartition() {
        if ("auto".equals(Constants.PARTITION_NUM)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(Constants.STREAM_NAME);
            this.partitionSize = partitionInfos.size();
        } else {
            this.partitionSize = Integer.valueOf(Constants.PARTITION_NUM);
        }
        LOGGER.info("Stream {} has {} partitions.", Constants.STREAM_NAME, partitionSize);
    }

    @Override
    public void run() {
        initPartition();
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (int i = 0; i < partitionSize; i++) {
            topicPartitions.add(new TopicPartition(Constants.STREAM_NAME, i));
        }
        consumer.assign(topicPartitions);
        if (PartitionCursorTypeEnum.TRIM_HORIZON == Constants.CONSUMER_CURSOR_TYPE) {
            consumer.seekToBeginning(topicPartitions);
        } else if (PartitionCursorTypeEnum.LATEST == Constants.CONSUMER_CURSOR_TYPE) {
            consumer.seekToEnd(topicPartitions);
        } else if (PartitionCursorTypeEnum.AT_SEQUENCE_NUMBER == Constants.CONSUMER_CURSOR_TYPE) {
            for (int i = 0; i < partitionSize; i++) {
                consumer.seek(topicPartitions.get(i), Constants.CONSUMER_OFFSET);
            }
        }

        ConsumerRecords<String, String> consumerRecords = null;
        while (true) {
            try {
                statistics.totalRequestTimes.addAndGet(this.partitionSize);
                long timeStart = System.currentTimeMillis();
                consumerRecords = consumer.poll(2000);
                long cost = System.currentTimeMillis() - timeStart;
                statistics.totalRequestSuccessTimes.addAndGet(this.partitionSize);
                statistics.totalSendSuccessRecords.addAndGet(consumerRecords.count());
                statistics.totalPostponeMillis.addAndGet(cost);
                if (LOGGER.isDebugEnabled()) {
                    outputData(consumerRecords);
                }
                TimeUnit.MILLISECONDS.sleep(Constants.SLEEP_TIME);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                statistics.totalRequestFailedTimes.incrementAndGet();
                try {
                    TimeUnit.MILLISECONDS.sleep(Constants.SERVER_FAILED_SLEEP_TIME);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    private void outputData(ConsumerRecords<String, String> consumerRecords) {
        if (consumerRecords == null || consumerRecords.count() == 0) {
            return;
        }
        for (ConsumerRecord<String, String> record : consumerRecords) {
            LOGGER.debug("Partition [{}], Content [{}], sequenceNumber [{}], timestamp [{}]",
                    record.partition(), record.value(), record.offset());
        }
    }
}