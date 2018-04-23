package com.bigdata.dis.sdk.demo.producer;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Scheduled;
import com.bigdata.dis.sdk.demo.data.RandomData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class AppProducer extends Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppProducer.class);

    private static Scheduled scheduled = new AppProducer();

    public void startThreads() {
        executorServicePool = Executors.newFixedThreadPool(Constants.THREAD_NUM);
        for (int threadIndex = 0; threadIndex < Constants.THREAD_NUM; threadIndex++) {
            executorServicePool.submit(new AppProducerThread(scheduled.statistics, new RandomData()));
        }
    }

    public static void main(String[] args) {
        scheduled.run();
    }
}