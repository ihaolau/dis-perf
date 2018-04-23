package com.bigdata.dis.sdk.demo.common;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.ThreadLocalRandom;

public class Public {
    public static SimpleDateFormat DATE_FORMATE = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

    public static String random(int bound) {
        return String.valueOf(nextInt(bound));
    }

    public static String randomKey() {
        return String.valueOf(nextInt(99999999));
    }

    public static String currentDateTime() {
        return DATE_FORMATE.format(Calendar.getInstance().getTime());
    }

    public static int nextInt(int bound) {
        return ThreadLocalRandom.current().nextInt(bound);
    }
}
