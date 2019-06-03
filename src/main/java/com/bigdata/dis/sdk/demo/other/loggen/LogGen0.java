package com.bigdata.dis.sdk.demo.other.loggen;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.data.IData;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 生产多个日志文件，每条记录唯一编号
 * 日志总条数通过producer_record_num控制(写多个日志或多线程情况下不精确，会略微多发几条，实际发送条数见程序结束时打印的Final totalCount)
 * 一条记录的长度通过producer_record_length控制
 * 输出格式由log4j2控制，默认写入logs/loggenX.log (X为0,1,2...)
 */
public class LogGen0 implements Runnable {
    private Logger LOG = LoggerFactory.getLogger(LogGen0.class);

    volatile static AtomicLong count = new AtomicLong(0);

    /**
     * 每条记录生成的间隔(0~N)
     */
    volatile static int sleepTime = 1;

    /**
     * 写日志的数量(1~5)
     */
    volatile static int logCount = 1;

    /**
     * 写每个日志的线程数量(1~N)
     */
    volatile static int threadCount = 1;

    volatile static boolean running = true;

    static IData data = Constants.PRODUCER_DATA_FACTORY;

    public static void main(String[] args) throws IllegalAccessException, InstantiationException {
        if (args != null && args.length > 0) {
            try {
                sleepTime = Integer.valueOf(args[0]);
                if (args.length > 1) {
                    logCount = Integer.valueOf(args[1]);
                }

                if (args.length > 2) {
                    threadCount = Integer.valueOf(args[2]);
                }
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                running = false;
                System.out.println("Final totalCount " + count.get());
            }
        });

        // 类名前缀(LogGen)
        String classPrefix = LogGen0.class.getName().substring(0, LogGen0.class.getName().length() - 1);
        // 启动线程生产日志
        for (int i = 0; i < logCount; i++) {
            Class<Runnable> aClass = null;
            try {
                aClass = (Class<Runnable>) Class.forName(classPrefix + i);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
            Thread loggen = new Thread(aClass.newInstance());
            loggen.start();
        }
    }

    public void run() {
        for (int i = 0; i < threadCount; i++) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (running) {
                        PutRecordsRequest request = data.createRequest("");
                        for (PutRecordsRequestEntry putRecordsRequestEntry : request.getRecords()) {
                            getLogger().info(new String(putRecordsRequestEntry.getData().array()));
                            long curCount = count.incrementAndGet();
                            if (Constants.PRODUCER_RECORD_NUM >= 0 && curCount >= Constants.PRODUCER_RECORD_NUM) {
                                System.exit(0);
                            }
                            if (sleepTime > 0) {
                                try {
                                    Thread.sleep(sleepTime);
                                } catch (InterruptedException ignored) {
                                }
                            }
                        }
                    }
                }
            });
            th.setName(getClass().getSimpleName() + String.format("-%02d", i));
            th.start();
        }
    }

    Logger getLogger() {
        return LOG;
    }
}