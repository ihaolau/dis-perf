package com.bigdata.dis.sdk.demo.common;

import com.bigdata.dis.sdk.demo.data.IData;
import com.bigdata.dis.sdk.demo.data.RandomData;
import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.iface.data.request.StreamType;
import com.huaweicloud.dis.util.PartitionCursorTypeEnum;
import org.apache.commons.lang3.RandomStringUtils;

public class Constants {

    public static final DISConfig DIS_CONFIG = DISConfig.buildDefaultConfig();

    public static final String STREAM_NAME = DIS_CONFIG.get("stream_name", null);
    public static final String MQTT_TOPIC_NAME = DIS_CONFIG.get("mqtt_topic_name", null);
    public static final String MQTT_USER_NAME = DIS_CONFIG.get("mqtt_user_name", null);
    public static final String MQTT_PASSWORD = DIS_CONFIG.get("mqtt_password", null);

    public static final long PRODUCER_RECORD_NUM = (DIS_CONFIG.get("producer_record_num", null) == null
            || DIS_CONFIG.getInt("producer_record_num", -1) < 0) ?
            Long.MAX_VALUE : Long.valueOf(DIS_CONFIG.get("producer_record_num", null).toString());
    public static final int PRODUCER_RECORD_LENGTH = DIS_CONFIG.getInt("producer_record_length", 1);
    public static final int PRODUCER_THREAD_NUM = DIS_CONFIG.getInt("producer_thread_num", 1);
    public static final int PRODUCER_REQUEST_RECORD_NUM = DIS_CONFIG.getInt("producer_request_record_num", 1);
    public static final long PRODUCER_REQUEST_SLEEP_TIME = DIS_CONFIG.getInt("producer_request_sleep_time", 0);

    public static final IData PRODUCER_DATA_FACTORY;
    // -2 for earliest; -1 for latest
    public static final int CONSUMER_OFFSET = DIS_CONFIG.getInt("consumer_offset", -1);
    public static final PartitionCursorTypeEnum CONSUMER_CURSOR_TYPE =
            CONSUMER_OFFSET == -2 ? PartitionCursorTypeEnum.TRIM_HORIZON :
                    CONSUMER_OFFSET == -1 ? PartitionCursorTypeEnum.LATEST :
                            CONSUMER_OFFSET >= 0 ? PartitionCursorTypeEnum.AT_SEQUENCE_NUMBER : null;
    public static final String CONSUMER_PARTITION_NUM = DIS_CONFIG.get("consumer_partition_num", "auto");
    public static final int CONSUMER_LIMIT = DIS_CONFIG.getInt("consumer_limit", 10000);
    public static final long CONSUMER_REQUEST_SLEEP_TIME = DIS_CONFIG.getInt("consumer_request_sleep_time", 0);

    public static final int CREATE_PARTITION_NUM = DIS_CONFIG.getInt("create_partition_num", 1);
    public static final String CREATE_STREAM_TYPE = DIS_CONFIG.get("create_stream_type", StreamType.ADVANCED.getType());
    public static final int CREATE_DATA_DURATION = DIS_CONFIG.getInt("create_data_duration", 7);
    public static final String CREATE_OBS_BUCKET = DIS_CONFIG.get("create_obs_bucket_name", null);
    public static final String CREATE_AGENCY_NAME = DIS_CONFIG.get("create_agency_name", null);
    public static final String CREATE_PARTITION_FORMAT = DIS_CONFIG.get("create_partition_format", null);
    public static final String CREATE_DELIVER_DATA_TYPE = DIS_CONFIG.get("create_deliver_data_type", null);

    public static final long AUTO_RUN_NUM = DIS_CONFIG.getInt("auto_run_num", 5);
    public static final String AUTO_RUN_USER_NAME = DIS_CONFIG.get("auto_run_user_name", "dis");

    public static final int CHECKPOINT_COMMIT_OFFSET = DIS_CONFIG.getInt("checkpoint_commit_offset", 0);
    public static final int CHECKPOINT_PARTITION_ID = DIS_CONFIG.getInt("checkpoint_partition_id", 0);
    public static final String CHECKPOINT_APP_NAME = DIS_CONFIG.get("checkpoint_app_name", null);

    public static final int UPDATE_PARTITION_COUNT = DIS_CONFIG.getInt("update_partition_count", 2);
    public static final int SERVER_FAILED_SLEEP_TIME = DIS_CONFIG.getInt("server_failed_sleep_time", 1000);
    public static final int DISPLAY_CONTENT_LIMIT = DIS_CONFIG.getInt("display_content_limit", 1024);

    public static final String MQTT_INSTANCE_NAME = DIS_CONFIG.get("mqtt_instance_name", "instance1");
    public static final String MQTT_CLIENT_ID = DIS_CONFIG.get("mqtt_client_id", RandomStringUtils.randomAlphanumeric(10));
    public static final String MQTT_BROKER = DIS_CONFIG.get("mqtt_broker", "tcp://49.4.52.253:8964");
    public static final int MQTT_QOS = DIS_CONFIG.getInt("mqtt_qos", 1);
    public static final int MQTT_SECURITY_MODE = DIS_CONFIG.getInt("mqtt_security_mode", 2);

    static {
        DIS_CONFIG.put(DISConfig.PROPERTY_BODY_SERIALIZE_TYPE, DIS_CONFIG.get("producer_body_serialize_type", "json"));
        try {
            PRODUCER_DATA_FACTORY = (IData) Class.forName(DIS_CONFIG.get("producer_data_factory", RandomData.class.getName())).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
