package com.bigdata.dis.sdk.demo.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaweicloud.dis.DIS;

/**
 * Delete APP Example
 */
public class DeleteApp
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteApp.class);
    
    public static void main(String[] args)
    {
        DIS dic = DISUtil.getInstance();
        String appName = DISUtil.getAppName();
        
        try
        {
            dic.deleteApp(appName);
            LOGGER.info("Success to delete app {}", appName);
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to delete app {}", appName, e);
        }
    }
}
