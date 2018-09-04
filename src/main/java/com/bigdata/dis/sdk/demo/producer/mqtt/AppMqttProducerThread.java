package com.bigdata.dis.sdk.demo.producer.mqtt;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Statistics;
import com.bigdata.dis.sdk.demo.data.IData;
import com.huaweicloud.dis.http.exception.ResourceAccessException;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.response.PutRecordsResult;
import org.apache.http.NoHttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

class AppMqttProducerThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppMqttProducerThread.class);

    private final MQTTClient dis;

    private IData data;

    private Statistics statistics;

    private String streamName;

    public AppMqttProducerThread(String streamName, Statistics statistics, IData data) {
        this.streamName = streamName;
        this.statistics = statistics;
        this.data = data;
        dis = new MQTTClient();
    }

    @Override
    public void run() {
        LOGGER.info("{}_{} start.", getName(), this.streamName);
        try {
            long requestNum = (long) Math.ceil(1.0 * Constants.PRODUCER_RECORD_NUM /
                    Constants.PRODUCER_REQUEST_RECORD_NUM / Constants.PRODUCER_THREAD_NUM);
            for (long loop = 0; loop < requestNum; loop++) {
                PutRecordsRequest putRecordsRequest = data.createRequest(this.streamName);
                statistics.totalRequestTimes.incrementAndGet();
                statistics.totalSendRecords.addAndGet(putRecordsRequest.getRecords().size());
                PutRecordsResult response = null;
                long timeStart = System.currentTimeMillis();
                try {
                    dis.putRecords(putRecordsRequest);
                    statistics.totalRequestSuccessTimes.incrementAndGet();
                    statistics.totalPostponeMillis.addAndGet(System.currentTimeMillis() - timeStart);
                } catch (Exception e) {
                    if (!(e instanceof ResourceAccessException && e.getCause() instanceof NoHttpResponseException)) {
                        statistics.totalRequestFailedTimes.incrementAndGet();
                        LOGGER.error("Failed put, cost " + (System.currentTimeMillis() - timeStart) + "ms. " + e.getMessage(), e);
                        statistics.totalSendFailedRecords.addAndGet(putRecordsRequest.getRecords().size());
                        TimeUnit.MILLISECONDS.sleep(Constants.SERVER_FAILED_SLEEP_TIME);
                    } else {
                        LOGGER.info(e.getMessage());
                    }
                }

                long success = putRecordsRequest.getRecords().size();
                long failed = 0;
                statistics.totalSendSuccessRecords.addAndGet(success);
                statistics.totalSendFailedRecords.addAndGet(failed);
                LOGGER.debug("CurrentPut {} records[success {} / failed {}] spend {} ms.",
                        putRecordsRequest.getRecords().size(), success, failed, System.currentTimeMillis() - timeStart);
                TimeUnit.MILLISECONDS.sleep(Constants.PRODUCER_REQUEST_SLEEP_TIME);
            }
        } catch (Exception e) {
            if (!(e instanceof InterruptedException)) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        LOGGER.info("{}_{} stop.", getName(), this.streamName);
    }
}