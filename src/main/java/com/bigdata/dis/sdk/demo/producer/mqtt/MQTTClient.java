package com.bigdata.dis.sdk.demo.producer.mqtt;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.data.custom.hr.EquInfo;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import com.huaweicloud.dis.util.JsonUtils;
import com.huaweicloud.dis.util.encrypt.Hex;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

public class MQTTClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(MQTTClient.class);

    public String broker = Constants.MQTT_BROKER;
    public String ak = Constants.DIS_CONFIG.getAK();
    public String sk = Constants.DIS_CONFIG.getSK();
    public String projectId = Constants.DIS_CONFIG.getProjectId();
    public String streamName = Constants.STREAM_NAME;
    public String mqttInstanceName = Constants.MQTT_INSTANCE_NAME;
    public String clientId = Constants.MQTT_CLIENT_ID;
    public int qos = Constants.MQTT_QOS;
    private MqttClient client = null;
    private String userName = null;
    private String password = null;

    public MQTTClient() {
        String[] userNameAndPassword = getUserNameAndPassword();
        this.userName = userNameAndPassword[0];
        this.password = userNameAndPassword[1];
        print(this);
        connect();
    }

    public MQTTClient(boolean init) {
        if (init) {
            connect();
        }
    }

    private String[] getUserNameAndPassword() {
        try {
            String[] result = new String[2];
            long timeStamp = System.currentTimeMillis();
            if (Constants.MQTT_SECURITY_MODE == 1) {
                String userName = String.format("%s|%s|%s|%s|%s", ak, projectId, mqttInstanceName, "1", timeStamp);
                String toHashStr = ak + projectId + mqttInstanceName + clientId + timeStamp;
                Mac mac = Mac.getInstance("HmacSHA256");
                mac.init(new SecretKeySpec(sk.getBytes(), "HmacSHA256"));
                String password = Hex.encodeHexStr(mac.doFinal(toHashStr.getBytes()));
                result[0] = userName;
                result[1] = password;
            } else if (Constants.MQTT_SECURITY_MODE == 2) {
                result[0] = String.format("%s|%s|%s|%s|%s", ak, projectId, mqttInstanceName, "2", timeStamp);
                result[1] = sk;
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void connect() {
        try {
            client = new MqttClient(broker, clientId, new MemoryPersistence());
            client.setCallback(new PushCallback());
            // 创建链接参数
            MqttConnectOptions connOpts = new MqttConnectOptions();

            if (!broker.startsWith("tcp")) {
//                connOpts.setSocketFactory(getSSLSocktet2(null, null, null, null));
                connOpts.setSocketFactory(SSLSocketFactory.getDefault());
            }

            // 在重新启动和重新连接时记住状态
            connOpts.setCleanSession(true);
            connOpts.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
            // 设置连接的用户名
            connOpts.setUserName(userName);
            connOpts.setPassword(password.toCharArray());
            // 建立连接
            client.connect(connOpts);
//            MqttTopic topic = client.getTopic(streamName);
        } catch (Exception e) {
            close();
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (client != null) {
            try {
                // 断开连接
                if (client.isConnected()) {
                    client.disconnect();
                }
                // 关闭客户端
                client.close();
            } catch (MqttException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    public void putRecords(PutRecordsRequest putRecordsRequest) {
        String streamName = putRecordsRequest.getStreamName();
        for (PutRecordsRequestEntry putRecordsRequestEntry : putRecordsRequest.getRecords()) {
            publish(streamName, putRecordsRequestEntry.getData().array());
        }
    }

    private void publish(String content) {
        publish(this.streamName, content.getBytes());
    }

    private void publish(String streamName, byte[] content) {
        if (client == null || !client.isConnected()) {
            LOGGER.error("MQTTClient is disconnected.");
            System.exit(1);
        }
        // 创建消息
        MqttMessage message = new MqttMessage(content);
        // 设置消息的服务质量
        message.setQos(qos);
        try {
            // 发布消息
            client.publish(streamName, message);
            LOGGER.debug("Success to push {}", message);
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    public SSLSocketFactory getSSLSocktet2(String caPath, String crtPath, String keyPath, String password)
            throws Exception {
        X509TrustManager x509m = new X509TrustManager() {

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain,
                                           String authType) throws CertificateException {
            }

            @Override
            public void checkClientTrusted(X509Certificate[] chain,
                                           String authType) throws CertificateException {
            }
        };
        // 获取一个SSLContext实例
        SSLContext sc = SSLContext.getInstance("SSL");
        // 初始化SSLContext实例
        sc.init(null, new TrustManager[]{x509m},
                new java.security.SecureRandom());

        SSLSocketFactory ssf = sc.getSocketFactory();
        return ssf;
    }


    public static void main(String[] args) {
        if (args != null && args.length > 0) {
            if ("genAuth".equalsIgnoreCase(args[0])) {
                MQTTClient client = new MQTTClient(false);
                String[] userNameAndPassword = client.getUserNameAndPassword();
                client.userName = userNameAndPassword[0];
                client.password = userNameAndPassword[1];
                print(client);
                return;
            }
        }
        MQTTClient client = new MQTTClient();
        try {
            while (true) {
                List<EquInfo> equInfos = EquInfo.getRandom();
                for (EquInfo info : equInfos) {
                    client.publish(JsonUtils.objToJson(info));
                }
                Thread.sleep(Constants.PRODUCER_REQUEST_SLEEP_TIME);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        client.close();
    }

    private static void print(MQTTClient client) {
        LOGGER.info("UserName: " + client.userName);
        LOGGER.info("Password: " + client.password);
        LOGGER.info("ClientId: " + client.clientId);
        LOGGER.info("MqttInstanceName: " + client.mqttInstanceName);
        LOGGER.info("Broker: " + client.broker);
        LOGGER.info("QosLevel: " + client.qos);
        LOGGER.info("Topic: " + client.streamName);
    }
}
