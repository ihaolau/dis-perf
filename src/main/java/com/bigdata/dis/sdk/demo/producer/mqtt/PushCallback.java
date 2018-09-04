package com.bigdata.dis.sdk.demo.producer.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushCallback implements MqttCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(PushCallback.class);

    public void connectionLost(Throwable cause) {
        // 连接丢失后，一般在这里面进行重连
        LOGGER.error("connectionLost.", cause);
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
        if (!token.isComplete()) {
            LOGGER.warn("Failed to complete {}", token);
        }
    }

    public void messageArrived(String topic, MqttMessage message) throws Exception {
        // subscribe后得到的消息会执行到这里面
        LOGGER.info("MessageArrived, Topic [{}], Qos [{}], Content [{}]", topic, message.getQos(), new String(message.getPayload()));
    }
}