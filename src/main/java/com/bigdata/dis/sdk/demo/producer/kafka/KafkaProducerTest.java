package com.bigdata.dis.sdk.demo.producer.kafka;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.data.IData;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Kafka发送端测试
 */
public class KafkaProducerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerTest.class);

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.99:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        IData producerDataFactory = Constants.PRODUCER_DATA_FACTORY;
        String topicName = "topic1";


        while (true) {
            PutRecordsRequest request = producerDataFactory.createRequest(topicName);
            for (PutRecordsRequestEntry putRecordsRequestEntry : request.getRecords()) {
                producer.send(new ProducerRecord<String, String>(topicName, putRecordsRequestEntry.getPartitionKey(), new String(putRecordsRequestEntry.getData().array())));
            }
            Thread.sleep(Constants.PRODUCER_REQUEST_SLEEP_TIME);
        }
    }
}