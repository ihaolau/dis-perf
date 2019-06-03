package com.bigdata.dis.sdk.demo.other.loggen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogGen3 extends LogGen0 implements Runnable {
    private Logger LOG = LoggerFactory.getLogger(LogGen3.class);

    Logger getLogger() {
        return LOG;
    }
}
