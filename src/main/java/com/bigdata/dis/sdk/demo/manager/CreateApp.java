package com.bigdata.dis.sdk.demo.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaweicloud.dis.DIS;

/**
 * Create APP Example
 */
public class CreateApp
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateApp.class);
    
    public static void main(String[] args)
    {
        DIS dic = DISUtil.getInstance();
        String appName = DISUtil.getAppName();
        
        try
        {
            dic.createApp(appName);
            LOGGER.info("Success to create app {}", appName);
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to create app {}", appName, e);
        }
    }
}
