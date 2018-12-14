package com.bigdata.dis.sdk.demo.manager;

import com.huaweicloud.dis.iface.app.response.DescribeAppResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DeleteAllApps extends DeleteApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteAllApps.class);

    public static void main(String[] args) {
        new DeleteAllApps().runAll();
    }

    public void runAll() {
        List<DescribeAppResult> apps = new ListApps().run();
        if (apps == null || apps.size() == 0) {
            LOGGER.info("No app.");
            return;
        }

        for (DescribeAppResult app : apps) {
            run(app.getAppName());
        }
    }
}
