package com.bigdata.dis.sdk.demo.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Statistics {
    private static final Logger LOGGER = LoggerFactory.getLogger(Statistics.class);

    public AtomicLong totalRequestTimes = new AtomicLong(0);
    public AtomicLong totalRequestSuccessTimes = new AtomicLong(0);
    public AtomicLong totalRequestFailedTimes = new AtomicLong(0);
    public AtomicLong totalSendRecords = new AtomicLong(0);
    public AtomicLong totalSendSuccessRecords = new AtomicLong(0);
    public AtomicLong totalSendFailedRecords = new AtomicLong(0);
    public AtomicLong totalPostponeMillis = new AtomicLong(0);
    public AtomicLong totalSendSuccessTraffic = new AtomicLong(0);

    public ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public long startTime;
    public DecimalFormat df = new DecimalFormat("0.00");

    public void start() {
        startTime = System.currentTimeMillis();
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            long lastCurrContinueSecond = (System.currentTimeMillis() - startTime) / 1000;
            long lastCurrTotalRequestTimes = totalRequestTimes.get();
            long lastCurrTotalRequestSuccessTimes = totalRequestSuccessTimes.get();
            long lastCurrTotalRequestFailedTimes = totalRequestFailedTimes.get();
            long lastCurrTotalSendRecords = totalSendRecords.get();
            long lastCurrTotalSendSuccessRecords = totalSendSuccessRecords.get();
            long lastCurrTotalSendFailedRecords = totalSendFailedRecords.get();
            long lastCurrTotalPostponeMillis = totalPostponeMillis.get();
            long lastCurrTotalSendSuccessTraffic = totalSendSuccessTraffic.get();

            @Override
            public void run() {
                try {
                    long currContinueSecond = (System.currentTimeMillis() - startTime) / 1000;
                    long currTotalRequestTimes = totalRequestTimes.get();
                    long currTotalRequestSuccessTimes = totalRequestSuccessTimes.get();
                    long currTotalRequestFailedTimes = totalRequestFailedTimes.get();
                    long currTotalSendRecords = totalSendRecords.get();
                    long currTotalSendSuccessRecords = totalSendSuccessRecords.get();
                    long currTotalSendFailedRecords = totalSendFailedRecords.get();
                    long currTotalPostponeMillis = totalPostponeMillis.get();
                    long currTotalSendSuccessTraffic = totalSendSuccessTraffic.get();

                    long currTPS = currTotalRequestSuccessTimes - lastCurrTotalRequestSuccessTimes;
                    LOGGER.info("TPS [{}] / [{}]({}/{}), " +
                                    "\tThroughput [{}] / [{}]({}/{}), " +
                                    "\tLatency [{}] / [{}]({}/{}), " +
                                    //"\tTraffic [{}] / [{}]({}/{}), " +
                                    "\tTotalRequestTimes [{}](success {} / failed {})" +
                                    ", TotalSendRecords [{}](success {} / failed {}).",
                            currTPS,
                            df.format(1f * currTotalRequestSuccessTimes / currContinueSecond), currTotalRequestSuccessTimes, currContinueSecond,
                            (currTotalSendSuccessRecords - lastCurrTotalSendSuccessRecords),
                            df.format(1f * currTotalSendSuccessRecords / currContinueSecond), currTotalSendSuccessRecords, currContinueSecond,
                            ((currTotalPostponeMillis - lastCurrTotalPostponeMillis) / (currTPS > 0 ? currTPS : 1)),
                            df.format(1f * currTotalPostponeMillis / currTotalRequestSuccessTimes), currTotalPostponeMillis, currTotalRequestSuccessTimes,
                            //(currTotalSendSuccessTraffic - lastCurrTotalSendSuccessTraffic),
                            //df.format(1f * currTotalSendSuccessTraffic / currTotalRequestTimes), currTotalSendSuccessTraffic, currTotalRequestTimes,
                            currTotalRequestTimes, currTotalRequestSuccessTimes, currTotalRequestFailedTimes,
                            currTotalSendRecords, currTotalSendSuccessRecords, currTotalSendFailedRecords
                    );

                    lastCurrContinueSecond = currContinueSecond;
                    lastCurrTotalRequestTimes = currTotalRequestTimes;
                    lastCurrTotalRequestSuccessTimes = currTotalRequestSuccessTimes;
                    lastCurrTotalRequestFailedTimes = currTotalRequestFailedTimes;
                    lastCurrTotalSendRecords = currTotalSendRecords;
                    lastCurrTotalSendSuccessRecords = currTotalSendSuccessRecords;
                    lastCurrTotalSendFailedRecords = currTotalSendFailedRecords;
                    lastCurrTotalPostponeMillis = currTotalPostponeMillis;
                    lastCurrTotalSendSuccessTraffic = currTotalSendSuccessTraffic;
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduledExecutorService.shutdown();
    }
}

