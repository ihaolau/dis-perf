package com.bigdata.dis.sdk.demo.producer.batch;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Scheduled;
import com.bigdata.dis.sdk.demo.data.RandomData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

public class AppProducerBatch extends Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppProducerBatch.class);

    public void startThreads(String streamName) {
        executorServicePool = Executors.newFixedThreadPool(Constants.PRODUCER_THREAD_NUM);
        executorServicePool.submit(new AppProducerThreadBatch(streamName, this.statistics, Constants.PRODUCER_DATA_FACTORY));
    }

    public static void main(String[] args) {
        new AppProducerBatch().run(Constants.STREAM_NAME);
    }

}