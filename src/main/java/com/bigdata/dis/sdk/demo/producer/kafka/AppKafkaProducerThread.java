package com.bigdata.dis.sdk.demo.producer.kafka;

import com.bigdata.dis.sdk.adapter.kafka.producer.DISKafkaProducer;
import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Statistics;
import com.bigdata.dis.sdk.demo.data.IData;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class AppKafkaProducerThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppKafkaProducerThread.class);

    private final Producer<String, byte[]> producer;

    private IData data;

    private Statistics statistics;

    private String streamName;

    public AppKafkaProducerThread(String streamName, Statistics statistics, IData data) {
        this.streamName = streamName;
        this.statistics = statistics;
        this.data = data;
        producer = new DISKafkaProducer<>(Constants.DIS_CONFIG);
        LOGGER.warn("DISKafkaProducer use 100 threads to put records, override producer_thread_num from {} to 100", Constants.PRODUCER_THREAD_NUM);
    }

    @Override
    public void run() {
        LOGGER.info("{}_{} start.", getName(), this.streamName);
        long requestNum = (long) Math.ceil(1.0 * Constants.PRODUCER_RECORD_NUM / Constants.PRODUCER_REQUEST_RECORD_NUM);
        long recordNum = requestNum * Constants.PRODUCER_REQUEST_RECORD_NUM;
        // TODO should be long
        int recordNumInt;
        if (recordNum <= Integer.MAX_VALUE) {
            recordNumInt = (int) recordNum;
        } else {
            requestNum = Integer.MAX_VALUE / Constants.PRODUCER_REQUEST_RECORD_NUM;
            recordNumInt = (int) (requestNum * Constants.PRODUCER_REQUEST_RECORD_NUM);
        }
        CountDownLatch countDownLatch = new CountDownLatch(recordNumInt);
        for (long loop = 0; loop < requestNum; loop++) {
            PutRecordsRequest putRecordsRequest = data.createRequest(this.streamName);
            statistics.totalSendRecords.addAndGet(putRecordsRequest.getRecords().size());

            for (PutRecordsRequestEntry putRecordsRequestEntry : putRecordsRequest.getRecords()) {
                ProducerRecord<String, byte[]> producerRecord =
                        new ProducerRecord<>(this.streamName,
                                putRecordsRequestEntry.getData().array());
                try {
                    producer.send(producerRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            countDownLatch.countDown();
                            if (e != null) {
                                statistics.totalSendFailedRecords.incrementAndGet();
                            } else {
                                statistics.totalSendSuccessRecords.incrementAndGet();
                            }
                        }
                    });

                    TimeUnit.MILLISECONDS.sleep(1);
                } catch (Exception e) {
                    countDownLatch.countDown();
                }
            }
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
        producer.close();
        LOGGER.info("{}_{} stop.", getName(), this.streamName);
    }
}