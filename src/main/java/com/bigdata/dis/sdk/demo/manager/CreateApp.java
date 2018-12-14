package com.bigdata.dis.sdk.demo.manager;

import com.huaweicloud.dis.DIS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create APP Example
 */
public class CreateApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateApp.class);

    public static void main(String[] args) {
        DIS dic = DISUtil.getInstance();
        String appName = DISUtil.getAppName();

        long start = System.currentTimeMillis();
        try {
            dic.createApp(appName);
            LOGGER.info("Success to create app [{}], cost {}ms", appName, (System.currentTimeMillis() - start));
        } catch (Exception e) {
            LOGGER.error("Failed to create app [{}]", appName, e);
        }
    }
}
