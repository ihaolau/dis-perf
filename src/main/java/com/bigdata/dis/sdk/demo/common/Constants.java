package com.bigdata.dis.sdk.demo.common;

import com.bigdata.dis.sdk.DISConfig;
import com.bigdata.dis.sdk.util.PartitionCursorTypeEnum;

public class Constants {

    public static final DISConfig DIS_CONFIG = DISConfig.buildDefaultConfig();

    public static final String STREAM_NAME = DIS_CONFIG.get("stream_name", null);
    public static final long SLEEP_TIME = DIS_CONFIG.getInt("sleep_time", 0);
    public static final int THREAD_NUM = DIS_CONFIG.getInt("thread_num", 1);
    public static final int RECORD_LENGTH = DIS_CONFIG.getInt("record_length", 1);
    public static final int RECORD_IN_REQUEST = DIS_CONFIG.getInt("record_in_request", 1);
    public static final int REQUEST_NUM = DIS_CONFIG.getInt("request_num", 1);
    // -2 for earliest; -1 for latest
    public static final int CONSUMER_OFFSET = DIS_CONFIG.getInt("consumer_offset", -1);

    public static final PartitionCursorTypeEnum CONSUMER_CURSOR_TYPE =
            CONSUMER_OFFSET == -2 ? PartitionCursorTypeEnum.TRIM_HORIZON :
                    CONSUMER_OFFSET == -1 ? PartitionCursorTypeEnum.LATEST :
                            CONSUMER_OFFSET > 0 ? PartitionCursorTypeEnum.AT_SEQUENCE_NUMBER : null;

    public static final int CONSUMER_LIMIT = DIS_CONFIG.getInt("consumer_limit", 10000);
    public static final int SERVER_FAILED_SLEEP_TIME = DIS_CONFIG.getInt("server_failed_sleep_time", 1000);
    public static final String PARTITION_NUM = DIS_CONFIG.get("partition_num", "auto");
}
