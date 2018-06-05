package com.bigdata.dis.sdk.demo.producer.batch;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Statistics;
import com.bigdata.dis.sdk.demo.data.IData;
import com.huaweicloud.dis.core.handler.AsyncHandler;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.response.PutRecordsResult;
import com.huaweicloud.dis.producer.DISProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * To be perfect coding.
 */
class AppProducerThreadBatch extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppProducerThreadBatch.class);

    private final DISProducer dis;

    private IData data;

    private Statistics statistics;

    private String streamName;

    private ExecutorService executorServicePool = new ThreadPoolExecutor(Constants.PRODUCER_THREAD_NUM, Constants.PRODUCER_THREAD_NUM,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(5000), Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());

    public AppProducerThreadBatch(String streamName, Statistics statistics, IData data) {
        this.streamName = streamName;
        this.statistics = statistics;
        this.data = data;
        dis = new DISProducer(Constants.DIS_CONFIG);
    }

    @Override
    public void run() {
        LOGGER.info("{}_{} start.", getName(), this.streamName);

        long requestNum = (long) Math.ceil(1.0 * Constants.PRODUCER_RECORD_NUM / Constants.PRODUCER_REQUEST_RECORD_NUM);
        for (long loop = 0; loop < requestNum; loop++) {
            PutRecordsRequest putRecordsRequest = data.createRequest(this.streamName);
            statistics.totalRequestTimes.incrementAndGet();
            statistics.totalSendRecords.addAndGet(putRecordsRequest.getRecords().size());
            long timeStart = System.currentTimeMillis();
            try {
                dis.putRecordsAsync(putRecordsRequest, new AsyncHandler<PutRecordsResult>() {
                    @Override
                    public void onError(Exception e) {
                        LOGGER.error(e.getMessage());
                        // TODO can get really send records.
                        statistics.totalSendFailedRecords.addAndGet(putRecordsRequest.getRecords().size());
                        statistics.totalRequestFailedTimes.incrementAndGet();
                        try {
                            TimeUnit.MILLISECONDS.sleep(Constants.SERVER_FAILED_SLEEP_TIME);
                            TimeUnit.MILLISECONDS.sleep(Constants.PRODUCER_REQUEST_SLEEP_TIME);
                        } catch (InterruptedException ignored) {
                        }
                    }

                    @Override
                    public void onSuccess(PutRecordsResult putRecordsResult) {
                        long cost = System.currentTimeMillis() - timeStart;
                        statistics.totalPostponeMillis.addAndGet(cost);

                        long success = putRecordsResult.getRecords().size() - putRecordsResult.getFailedRecordCount().get();
                        long failed = putRecordsResult.getFailedRecordCount().longValue();
                        statistics.totalSendSuccessRecords.addAndGet(success);
                        statistics.totalSendFailedRecords.addAndGet(failed);
                        statistics.totalRequestSuccessTimes.incrementAndGet();
                        statistics.totalPostponeMillis.addAndGet(System.currentTimeMillis() - timeStart);
                        try {
                            TimeUnit.MILLISECONDS.sleep(Constants.PRODUCER_REQUEST_SLEEP_TIME);
                        } catch (InterruptedException ignored) {
                        }
                    }
                });
                TimeUnit.MILLISECONDS.sleep(1);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                statistics.totalSendFailedRecords.addAndGet(putRecordsRequest.getRecords().size());
                statistics.totalRequestFailedTimes.incrementAndGet();
            }
        }
        try {
            executorServicePool.shutdown();
            executorServicePool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            dis.close();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        LOGGER.info("{}_{} stop.", getName(), this.streamName);
    }
}