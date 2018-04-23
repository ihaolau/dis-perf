package com.bigdata.dis.sdk.demo.consumer;

import com.bigdata.dis.sdk.demo.common.Scheduled;
import com.bigdata.dis.sdk.demo.producer.AppProducer;
import com.bigdata.dis.sdk.demo.producer.async.AppProducerAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AppConsumer extends Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppConsumer.class);

    private static Scheduled scheduled = new AppConsumer();

    @Override
    public void startThreads() {
        executorServicePool = Executors.newSingleThreadExecutor();
        executorServicePool.submit(new AppConsumerThread(scheduled.statistics));
    }

    public static void main(String[] args) {
        scheduled.run();
    }
}