package com.bigdata.dis.sdk.demo.producer.mqtt;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

public class AppMqttProducer extends Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppMqttProducer.class);

    public static void main(String[] args) {
        new AppMqttProducer().run(Constants.STREAM_NAME);
    }

    public void startThreads(String streamName) {
        executorServicePool = Executors.newFixedThreadPool(Constants.PRODUCER_THREAD_NUM);
        for (int threadIndex = 0; threadIndex < Constants.PRODUCER_THREAD_NUM; threadIndex++) {
            executorServicePool.submit(new AppMqttProducerThread(streamName , this.statistics, Constants.PRODUCER_DATA_FACTORY));
        }
    }
}