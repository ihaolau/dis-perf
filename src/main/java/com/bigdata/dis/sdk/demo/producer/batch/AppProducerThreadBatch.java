package com.bigdata.dis.sdk.demo.producer.batch;

import com.bigdata.dis.data.iface.request.PutRecordsRequest;
import com.bigdata.dis.data.iface.response.PutRecordsResult;
import com.bigdata.dis.sdk.core.handler.AsyncHandler;
import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Statistics;
import com.bigdata.dis.sdk.demo.data.IData;
import com.bigdata.dis.sdk.exception.DISClientException;
import com.bigdata.dis.sdk.producer.DISProducer;
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

    private ExecutorService executorServicePool = new ThreadPoolExecutor(Constants.THREAD_NUM, Constants.THREAD_NUM,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(5000), Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());

    public AppProducerThreadBatch(Statistics statistics, IData data) {
        this.statistics = statistics;
        this.data = data;
        dis = new DISProducer(Constants.DIS_CONFIG);
    }

    @Override
    public void run() {
        LOGGER.info("Thread {} start.", getName());
        try {
            for (int loop = 0; loop < Constants.REQUEST_NUM; loop++) {
                PutRecordsRequest putRecordsRequest = data.createRequest();
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
                                TimeUnit.MILLISECONDS.sleep(Constants.SLEEP_TIME);
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
                                TimeUnit.MILLISECONDS.sleep(Constants.SLEEP_TIME);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    });
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                    statistics.totalSendFailedRecords.addAndGet(putRecordsRequest.getRecords().size());
                    statistics.totalRequestFailedTimes.incrementAndGet();
                }
                // TimeUnit.MILLISECONDS.sleep(1);
            }
            executorServicePool.shutdown();
            executorServicePool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}