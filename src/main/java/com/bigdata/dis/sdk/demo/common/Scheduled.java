package com.bigdata.dis.sdk.demo.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(Scheduled.class);
    public ExecutorService executorServicePool = null;
    public Statistics statistics = new Statistics();

    public abstract void startThreads(String streamName);

    public void run(String streamName) {
        try {
            startThreads(streamName);
            statistics.start();
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    if (executorServicePool != null) {
                        executorServicePool.shutdownNow();
                    }

                    if (statistics != null) {
                        try {
                            Thread.sleep(1500);
                        } catch (InterruptedException e) {
                            LOGGER.error(e.getMessage(), e);
                        }
                        statistics.stop();
                    }
                }
            }));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        waitShutdown();
    }

    public void waitShutdown() {
        try {
            executorServicePool.shutdown();
            executorServicePool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            Thread.sleep(1500);
            statistics.stop();
            executorServicePool = null;
            statistics = null;
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
