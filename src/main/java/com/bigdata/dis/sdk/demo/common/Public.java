package com.bigdata.dis.sdk.demo.common;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class Public {
    public static SimpleDateFormat DATE_FORMATE = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

    public static DecimalFormat DF3 = new DecimalFormat("#0.000");

    public static DecimalFormat DF2 = new DecimalFormat("#0.00");

    public static DecimalFormat DF1 = new DecimalFormat("#0.0");

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

    public static double nextDouble(double bound, int length) {
        if (length == 3) {
            return Double.parseDouble(DF3.format(ThreadLocalRandom.current().nextDouble(bound)));
        } else if (length == 2) {
            return Double.parseDouble(DF2.format(ThreadLocalRandom.current().nextDouble(bound)));
        } else {
            return Double.parseDouble(DF1.format(ThreadLocalRandom.current().nextDouble(bound)));
        }
    }

    public static double DF3(double d) {
        return Double.parseDouble(DF3.format(d));
    }

    public static double DF2(double d) {
        return Double.parseDouble(DF2.format(d));
    }

    public static double DF1(double d) {
        return Double.parseDouble(DF1.format(d));
    }

    public static double rDouble(double bound, int length) {
        int n = nextInt(10);
        double d = nextDouble(bound, length);
        if (n == 1 || n == 3 || n % 2 == 0) {
            return -d;
        }
        return d;
    }

    public static void main(String[] args) {
        System.out.println(Public.nextInt(1));
    }
}
