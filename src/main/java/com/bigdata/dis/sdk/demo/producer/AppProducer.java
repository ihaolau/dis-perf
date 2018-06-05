package com.bigdata.dis.sdk.demo.producer;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Scheduled;
import com.bigdata.dis.sdk.demo.data.RandomData;
import com.bigdata.dis.sdk.demo.data.custom.hr.HRData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

public class AppProducer extends Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppProducer.class);

    public void startThreads(String streamName) {
        executorServicePool = Executors.newFixedThreadPool(Constants.PRODUCER_THREAD_NUM);
        for (int threadIndex = 0; threadIndex < Constants.PRODUCER_THREAD_NUM; threadIndex++) {
            executorServicePool.submit(new AppProducerThread(streamName, this.statistics, new HRData()));
        }
    }

    public static void main(String[] args) {
        new AppProducer().run(Constants.STREAM_NAME);
    }
}