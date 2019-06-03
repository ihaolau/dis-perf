package com.bigdata.dis.sdk.demo.manager;

import com.huaweicloud.dis.DIS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Describe APP Example
 */
public class DescribeApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(DescribeApp.class);

    public static void main(String[] args) {
        DIS dic = DISUtil.getInstance();
        String appName = DISUtil.getAppName();

        long start = System.currentTimeMillis();
        try {
            dic.describeApp(appName);
            LOGGER.info("Success to describe app [{}], cost {}ms", appName, (System.currentTimeMillis() - start));
        } catch (Exception e) {
            LOGGER.error("Failed to describe app [{}]", appName, e);
        }
    }
}
