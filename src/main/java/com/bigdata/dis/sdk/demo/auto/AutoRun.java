package com.bigdata.dis.sdk.demo.auto;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.manager.CreateStream;
import com.bigdata.dis.sdk.demo.manager.DeleteStream;
import com.bigdata.dis.sdk.demo.manager.DescribeStream;
import com.bigdata.dis.sdk.demo.producer.AppProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

public class AutoRun {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoRun.class);

    public static SimpleDateFormat DATE_FORMATE = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");

    public static void main(String[] args) {
        long autoRunNum = Constants.AUTO_RUN_NUM;
        if (autoRunNum < 0) {
            autoRunNum = Long.MAX_VALUE;
        }
        for (long i = 1; i <= autoRunNum; i++) {
            long start = System.currentTimeMillis();
            String streamName = "AutoRun_" + Constants.AUTO_RUN_USER_NAME + "_" +
                    DATE_FORMATE.format(Calendar.getInstance().getTime()) + "_" + System.currentTimeMillis();
            try {
                LOGGER.info("Start to auto run {}, streamName {}", i, streamName);
                new CreateStream().run(streamName, true);
                new AppProducer().run(streamName);
                new DescribeStream().run(streamName);
                new DeleteStream().run(streamName);
            } catch (Exception e) {
                LOGGER.error("Run " + streamName + " failed, " + e.getMessage().replaceAll("\n", "\\\\n"), e);
                try {
                    TimeUnit.SECONDS.sleep(10);
                    new DeleteStream().run(streamName);
                } catch (Exception ignore) {
                }
            } finally {
                LOGGER.info("End to auto run {}, streamName {}, cost {}ms", i, streamName, (System.currentTimeMillis() - start));
            }
        }
    }
}
