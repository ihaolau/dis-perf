package com.bigdata.dis.sdk.demo.manager;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.huaweicloud.dis.DIS;
import com.huaweicloud.dis.DISClient;

public class DISUtil
{
    private static DIS dic;
    
    private static String STREAM_NAME = Constants.STREAM_NAME;
    
    private static String APP_NAME = Constants.APP_NAME;
    
    private DISUtil()
    {
        
    }
    
    public static DIS getInstance()
    {
        if (dic == null)
        {
            synchronized (DISUtil.class)
            {
                if (dic == null)
                {
                    dic = createDISClient();
                }
            }
            
        }
        return dic;
    }
    
    public static String getStreamName()
    {
        return STREAM_NAME;
    }
    
    public static String getAppName()
    {
        return APP_NAME;
    }
    
    private static DIS createDISClient()
    {
        return new DISClient(Constants.DIS_CONFIG);
    }
}
