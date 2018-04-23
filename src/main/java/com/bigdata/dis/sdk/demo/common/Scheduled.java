package com.bigdata.dis.sdk.demo.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(Scheduled.class);
    public ExecutorService executorServicePool = Executors.newFixedThreadPool(Constants.THREAD_NUM);
    public Statistics statistics = new Statistics();

    public abstract void startThreads();

    public void run() {
        startThreads();
        statistics.start();
        waitShutdown();
    }

    public void waitShutdown() {
        try {
            executorServicePool.shutdown();
            executorServicePool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            Thread.sleep(1500);
            statistics.stop();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
