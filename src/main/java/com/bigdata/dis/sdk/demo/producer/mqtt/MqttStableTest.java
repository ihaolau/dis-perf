package com.bigdata.dis.sdk.demo.producer.mqtt;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Scheduled;
import com.huaweicloud.dis.DIS;
import com.huaweicloud.dis.DISClient;
import com.huaweicloud.dis.iface.stream.request.DescribeStreamRequest;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import com.huaweicloud.dis.iface.stream.response.PartitionResult;
import com.huaweicloud.dis.iface.transfertask.request.DescribeTransferTaskRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MqttStableTest extends Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(MqttStableTest.class);

    public static void main(String[] args) {
        String streamName=Constants.STREAM_NAME;
        List<Boolean> round= new ArrayList<Boolean>();
        long totalRecords=100;
        int onceRecords=100;
        int currentNumber=0;
        int lastTotalNumber=0;
        int times=1;
        lastTotalNumber=getNumberOfRecords(streamName);

        while (true) {
            totalRecords=lastTotalNumber+onceRecords;
            new MqttStableTest().run(Constants.MQTT_TOPIC_NAME);
            currentNumber=getNumberOfRecords(streamName);
            int i=0;
            if ((totalRecords-currentNumber)<=onceRecords&&i<100){
                try {
                    Thread.sleep(1000);
                    currentNumber=getNumberOfRecords(streamName);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                i++;
            }
            if (currentNumber==totalRecords){
                round.add(true);
            }else {
                round.add(false);
            }
            LOGGER.info("the transfertask's result of round {} is {},expect transfer count is {}, actual transfer count is {}"
                    ,times,round.get(times-1),totalRecords,currentNumber);
            lastTotalNumber=currentNumber;
            times++;
        }
    }

    public static int getNumberOfRecords(String streamName){
        int currentNumber=0;
        DIS dis = new DISClient(Constants.DIS_CONFIG);
        DescribeStreamRequest describeStreamRequest=new DescribeStreamRequest();
        DescribeStreamResult describeStreamResult=new DescribeStreamResult();
        describeStreamRequest.setStreamName(streamName);
        describeStreamResult =dis.describeStream(describeStreamRequest);
        for (PartitionResult partitionResult:describeStreamResult.getPartitions()) {
            String sequenceNumber = partitionResult.getSequenceNumberRange();
            currentNumber += Integer.valueOf(getNumbers(sequenceNumber,2));
        }
        return currentNumber;
    }

    public static String getNumbers(String content,int position) {
        Pattern pattern = Pattern.compile("\\d+");
        Matcher matcher = pattern.matcher(content);
        int i=1;
        while (matcher.find()) {
            if (i==position) {
                return matcher.group();
            }
            i++;
        }
        return "";
    }

    public void startThreads(String streamName) {
        executorServicePool = Executors.newFixedThreadPool(Constants.PRODUCER_THREAD_NUM);
        for (int threadIndex = 0; threadIndex < Constants.PRODUCER_THREAD_NUM; threadIndex++) {
            executorServicePool.submit(new AppMqttProducerThread(streamName + threadIndex, this.statistics, Constants.PRODUCER_DATA_FACTORY));
        }
    }
}