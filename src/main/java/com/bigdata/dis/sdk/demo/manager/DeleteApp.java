package com.bigdata.dis.sdk.demo.manager;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.DIS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delete APP Example
 */
public class DeleteApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteApp.class);

    public static void main(String[] args) {
        new DeleteApp().run(Constants.CHECKPOINT_APP_NAME);
    }

    public void run(String appName) {
        DIS dic = DISUtil.getInstance();
        long start = System.currentTimeMillis();
        try {
            dic.deleteApp(appName);
            LOGGER.info("Success to delete app [{}], cost {}ms", appName, (System.currentTimeMillis() - start));
        } catch (Exception e) {
            LOGGER.error("Failed to delete app [{}]", appName, e);
        }
    }
}
