package com.bigdata.dis.sdk.demo.producer.async;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Scheduled;
import com.bigdata.dis.sdk.demo.data.RandomData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

public class AppProducerAsync extends Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppProducerAsync.class);

    private static Scheduled scheduled = new AppProducerAsync();

    public void startThreads() {
        executorServicePool = Executors.newFixedThreadPool(Constants.THREAD_NUM);
        executorServicePool.submit(new AppProducerThreadAsync(scheduled.statistics, new RandomData()));
    }

    public static void main(String[] args) {
        scheduled.run();
    }
}