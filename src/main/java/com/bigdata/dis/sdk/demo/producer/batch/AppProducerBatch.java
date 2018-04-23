package com.bigdata.dis.sdk.demo.producer.batch;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Scheduled;
import com.bigdata.dis.sdk.demo.data.RandomData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

public class AppProducerBatch extends Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppProducerBatch.class);

    private static Scheduled scheduled = new AppProducerBatch();

    public void startThreads() {
        executorServicePool = Executors.newFixedThreadPool(Constants.THREAD_NUM);
        executorServicePool.submit(new AppProducerThreadBatch(scheduled.statistics, new RandomData()));
    }

    public static void main(String[] args) {
        scheduled.run();
    }
}