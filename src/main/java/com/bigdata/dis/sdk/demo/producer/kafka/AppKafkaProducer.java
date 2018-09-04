package com.bigdata.dis.sdk.demo.producer.kafka;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Scheduled;
import com.bigdata.dis.sdk.demo.data.RandomData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

public class AppKafkaProducer extends Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppKafkaProducer.class);

    private static Scheduled scheduled = new AppKafkaProducer();

    public void startThreads(String streamName) {
        executorServicePool = Executors.newFixedThreadPool(Constants.PRODUCER_THREAD_NUM);
        executorServicePool.submit(new AppKafkaProducerThread(streamName, scheduled.statistics, Constants.PRODUCER_DATA_FACTORY));
    }

    public static void main(String[] args) {
        scheduled.run(Constants.STREAM_NAME);
    }
}