package com.bigdata.dis.sdk.demo.consumer.kafka;

import com.bigdata.dis.sdk.demo.common.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

public class AppKafkaConsumer extends Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppKafkaConsumer.class);

    private static Scheduled scheduled = new AppKafkaConsumer();

    @Override
    public void startThreads() {
        executorServicePool = Executors.newSingleThreadExecutor();
        executorServicePool.submit(new AppKafkaConsumerThread(scheduled.statistics));
    }

    public static void main(String[] args) {
        scheduled.run();
    }
}