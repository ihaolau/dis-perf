package com.bigdata.dis.sdk.demo.manager;

import com.bigdata.dis.sdk.demo.common.Constants;
import com.bigdata.dis.sdk.demo.common.Public;
import com.huaweicloud.dis.DISClient;
import com.huaweicloud.dis.iface.app.request.ListAppsRequest;
import com.huaweicloud.dis.iface.app.response.DescribeAppResult;
import com.huaweicloud.dis.iface.app.response.ListAppsResult;
import com.huaweicloud.dis.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ListApps {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListApps.class);

    public static void main(String[] args) {
        new ListApps().run();
    }

    public List<DescribeAppResult> run() {
        DISClient disClient = new DISClient(Constants.DIS_CONFIG);

        ListAppsRequest listAppsRequest = new ListAppsRequest();
        listAppsRequest.setLimit(100);

        String startStreamName = null;
        ListAppsResult listAppsResult = null;
        List<DescribeAppResult> apps = new ArrayList<>();
        long start = System.currentTimeMillis();
        do {
            listAppsRequest.setExclusiveStartAppName(startStreamName);
            listAppsResult = disClient.listApps(listAppsRequest);
            apps.addAll(listAppsResult.getApps());
            if (apps.size() > 0) {
                startStreamName = apps.get(apps.size() - 1).getAppName();
            }
        }
        while (listAppsResult.getHasMoreApp());

        if (apps.size() > 0) {
            apps.sort(new Comparator<DescribeAppResult>() {
                @Override
                public int compare(DescribeAppResult o1, DescribeAppResult o2) {
                    return Long.compare(o1.getCreateTime(), o2.getCreateTime());
                }
            });
        }
        for (int i = 1; i <= apps.size(); i++) {
            DescribeAppResult app = apps.get(i - 1);
            LOGGER.info("{}\t\t {} [{}]", i, JsonUtils.objToJson(app), Public.formatTimestamp(app.getCreateTime()));
        }
        LOGGER.info("Success to list {} Apps, cost {}ms", apps.size(), (System.currentTimeMillis() - start));
        return apps;
    }
}
