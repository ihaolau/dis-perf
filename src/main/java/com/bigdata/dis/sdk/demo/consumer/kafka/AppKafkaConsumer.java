package com.bigdata.dis.sdk.demo.consumer.kafka;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

public class AppKafkaConsumer extends Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppKafkaConsumer.class);

    public static void main(String[] args) {
        new AppKafkaConsumer().run(Constants.STREAM_NAME);
    }

    @Override
    public void startThreads(String streamName) {
        executorServicePool = Executors.newSingleThreadExecutor();
        executorServicePool.submit(new AppKafkaConsumerThread(streamName, this.statistics));
    }
}