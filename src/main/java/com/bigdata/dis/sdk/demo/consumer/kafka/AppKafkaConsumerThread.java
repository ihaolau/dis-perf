package com.bigdata.dis.sdk.demo.consumer.kafka;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Statistics;
import com.huaweicloud.dis.adapter.kafka.consumer.DISKafkaConsumer;
import com.huaweicloud.dis.util.PartitionCursorTypeEnum;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

class AppKafkaConsumerThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppKafkaConsumerThread.class);

    private final Consumer<String, String> consumer;

    private int partitionSize = 0;

    private Statistics statistics;

    private String streamName;

    public AppKafkaConsumerThread(String streamName, Statistics statistics) {
        this.streamName = streamName;
        this.statistics = statistics;
        consumer = new DISKafkaConsumer<>(Constants.DIS_CONFIG);
    }

    public void initPartition() {
        if ("auto".equals(Constants.CONSUMER_PARTITION_NUM)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(this.streamName);
            this.partitionSize = partitionInfos.size();
        } else {
            this.partitionSize = Integer.valueOf(Constants.CONSUMER_PARTITION_NUM);
        }
        LOGGER.info("Stream {} has {} partitions.", this.streamName, partitionSize);
    }

    @Override
    public void run() {
        initPartition();
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (int i = 0; i < partitionSize; i++) {
            topicPartitions.add(new TopicPartition(this.streamName, i));
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
                TimeUnit.MILLISECONDS.sleep(Constants.CONSUMER_REQUEST_SLEEP_TIME);
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