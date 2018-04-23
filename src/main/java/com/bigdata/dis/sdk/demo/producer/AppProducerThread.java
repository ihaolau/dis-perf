package com.bigdata.dis.sdk.demo.producer;

import com.bigdata.dis.data.iface.request.PutRecordsRequest;
import com.bigdata.dis.data.iface.response.PutRecordsResult;
import com.bigdata.dis.sdk.DIS;
import com.bigdata.dis.sdk.DISClient;
import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Statistics;
import com.bigdata.dis.sdk.demo.data.IData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

class AppProducerThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppProducerThread.class);

    private final DIS dis;

    private IData data;

    private Statistics statistics;

    public AppProducerThread(Statistics statistics, IData data) {
        this.statistics = statistics;
        this.data = data;
        dis = new DISClient(Constants.DIS_CONFIG);
    }

    @Override
    public void run() {
        LOGGER.info("{} start.", getName());
        try {
            for (int loop = 0; loop < Constants.REQUEST_NUM; loop++) {
                PutRecordsRequest putRecordsRequest = data.createRequest();
                statistics.totalRequestTimes.incrementAndGet();
                statistics.totalSendRecords.addAndGet(putRecordsRequest.getRecords().size());
                PutRecordsResult response = null;
                long timeStart = System.currentTimeMillis();
                try {
                    response = dis.putRecords(putRecordsRequest);
                    statistics.totalRequestSuccessTimes.incrementAndGet();
                    statistics.totalPostponeMillis.addAndGet(System.currentTimeMillis() - timeStart);
                } catch (Exception e) {
                    statistics.totalRequestFailedTimes.incrementAndGet();
                    LOGGER.error("Failed put, cost " + (System.currentTimeMillis() - timeStart) + "ms. " + e.getMessage(), e);
                    TimeUnit.MILLISECONDS.sleep(Constants.SERVER_FAILED_SLEEP_TIME);
                }

                if (response != null) {
                    long success = putRecordsRequest.getRecords().size() - response.getFailedRecordCount().longValue();
                    long failed = response.getFailedRecordCount().longValue();
                    statistics.totalSendSuccessRecords.addAndGet(success);
                    statistics.totalSendFailedRecords.addAndGet(failed);
                    LOGGER.debug("CurrentPut {} records[success {} / failed {}] spend {} ms.",
                            putRecordsRequest.getRecords().size(), success, failed, System.currentTimeMillis() - timeStart);
                } else {
                    statistics.totalSendFailedRecords.addAndGet(putRecordsRequest.getRecords().size());
                }

                TimeUnit.MILLISECONDS.sleep(Constants.SLEEP_TIME);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}