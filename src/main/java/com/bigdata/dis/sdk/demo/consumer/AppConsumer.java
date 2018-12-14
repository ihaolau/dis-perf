package com.bigdata.dis.sdk.demo.consumer;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

public class AppConsumer extends Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppConsumer.class);

    public static void main(String[] args) {
        new AppConsumer().run(Constants.STREAM_NAME);
    }

    @Override
    public void startThreads(String streamName) {
        executorServicePool = Executors.newSingleThreadExecutor();
        executorServicePool.submit(new AppConsumerThread(streamName, this.statistics));
    }
}