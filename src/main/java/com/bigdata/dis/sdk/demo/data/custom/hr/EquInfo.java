package com.bigdata.dis.sdk.demo.data.custom.hr;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Public;
import com.bigdata.dis.sdk.demo.producer.AppProducer;
import com.huaweicloud.dis.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class EquInfo implements Cloneable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EquInfo.class);

    static SimpleDateFormat sf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    static DecimalFormat df = new DecimalFormat("#0.00");
    private String equId;
    private String equType;
    private String zoneId;
    private long uploadTime;
    private double runningTime;
    private double temperature;
    private double pressure;
    private double waterLine;
    private double targetTemperature;
    private double targetPressure;
    private double targetWaterLine;
    private double feedWater;
    private double noxEmissions;
    private double unitLoad;

    private static Map<String, EquInfo> equInfo = new ConcurrentHashMap<>();
    private static volatile boolean init = false;

    public static List<EquInfo> getRandom() {
        if (!init) {
            init();
        }

        List<EquInfo> equInfos = new ArrayList<>(equInfo.size());
        for (EquInfo oldInfo : equInfo.values()) {
            EquInfo info = null;
            try {
                info = (EquInfo) oldInfo.clone();
            } catch (CloneNotSupportedException e) {
                LOGGER.error(e.getMessage(), e);
            }

            if ("mock".equals(Constants.DIS_CONFIG.get("data.timestamp", "mock"))) {
                info.uploadTime = info.uploadTime + 1000;
            } else {
                info.uploadTime = System.currentTimeMillis();
            }
            info.runningTime = Double.parseDouble(Public.DF3.format(info.runningTime + 1.0 / 3600));

            double temp = info.temperature + Public.rDouble(10, 1);
            info.temperature = Public.DF1(temp > 0 ? temp : (50 + Public.nextInt(10)));

            double pres = info.pressure + Public.rDouble(1, 2);
            info.pressure = Public.DF2(pres > 0 ? pres : (1 + Public.nextDouble(1, 2)));

            double water = info.waterLine + Public.rDouble(10, 1);
            info.waterLine = Public.DF1(water > 0 ? water : (30 + Public.nextInt(10)));

            double feed = info.feedWater + Public.rDouble(1, 3);
            info.feedWater = Public.DF3(feed > 0 ? feed : (Public.nextDouble(1, 3)));

            double nox = info.noxEmissions + Public.rDouble(10, 1);
            info.noxEmissions = Public.DF1(nox > 0 ? nox : (30 + Public.nextDouble(20, 1)));

            double unit = Public.DF1(info.unitLoad + Public.rDouble(10, 1));
            info.unitLoad = unit > 0 ? unit : (Public.nextInt(100));
            if (info.unitLoad > 100) {
                info.unitLoad = Public.nextInt(100);
            }
            equInfos.add(info);
            equInfo.put(info.getEquId(), info);
        }

        return equInfos;
    }

    public static synchronized void init() {
        if (init) {
            return;
        }

        int equIdNum = 10 + Public.nextInt(10);
        for (int i = 0; i < equIdNum; i++) {
            EquInfo l = new EquInfo();
            l.equId = String.valueOf(i);
            l.equType = Public.random(3);
            l.zoneId = Public.random(5);
            l.uploadTime = System.currentTimeMillis() - 30 * 60 * 1000;
            l.runningTime = Public.nextInt(100);
            l.temperature = 50 + Public.nextInt(10);
            l.pressure = Public.DF2(1.0 + Public.nextDouble(1, 2));
            l.waterLine = 30 + Public.nextInt(10);
            l.targetTemperature = 60 + Public.nextInt(40);
            l.targetPressure = Public.DF2(4.0 + Public.nextDouble(1, 2));
            l.targetWaterLine = 30 + Public.nextInt(10);
            l.feedWater = Public.DF3(Public.nextDouble(3, 3));
            l.noxEmissions = Public.DF1(30 + Public.nextDouble(20, 1));
            l.unitLoad = Public.nextInt(100);
            equInfo.put(l.getEquId(), l);
        }
        init = true;
    }

    public static void main(String[] args) throws CloneNotSupportedException {

    }

    public String toString() {
        return JsonUtils.objToJson(this);
    }

    public String getEquId() {
        return equId;
    }

    public void setEquId(String equId) {
        this.equId = equId;
    }

    public String getEquType() {
        return equType;
    }

    public void setEquType(String equType) {
        this.equType = equType;
    }

    public String getZoneId() {
        return zoneId;
    }

    public void setZoneId(String zoneId) {
        this.zoneId = zoneId;
    }

    public long getUploadTime() {
        return uploadTime;
    }

    public void setUploadTime(long uploadTime) {
        this.uploadTime = uploadTime;
    }

    public double getRunningTime() {
        return runningTime;
    }

    public void setRunningTime(double runningTime) {
        this.runningTime = runningTime;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public double getPressure() {
        return pressure;
    }

    public void setPressure(double pressure) {
        this.pressure = pressure;
    }

    public double getWaterLine() {
        return waterLine;
    }

    public void setWaterLine(double waterLine) {
        this.waterLine = waterLine;
    }

    public double getTargetTemperature() {
        return targetTemperature;
    }

    public void setTargetTemperature(double targetTemperature) {
        this.targetTemperature = targetTemperature;
    }

    public double getTargetPressure() {
        return targetPressure;
    }

    public void setTargetPressure(double targetPressure) {
        this.targetPressure = targetPressure;
    }

    public double getTargetWaterLine() {
        return targetWaterLine;
    }

    public void setTargetWaterLine(double targetWaterLine) {
        this.targetWaterLine = targetWaterLine;
    }

    public double getFeedWater() {
        return feedWater;
    }

    public void setFeedWater(double feedWater) {
        this.feedWater = feedWater;
    }

    public double getNoxEmissions() {
        return noxEmissions;
    }

    public void setNoxEmissions(double noxEmissions) {
        this.noxEmissions = noxEmissions;
    }

    public double getUnitLoad() {
        return unitLoad;
    }

    public void setUnitLoad(double unitLoad) {
        this.unitLoad = unitLoad;
    }
}