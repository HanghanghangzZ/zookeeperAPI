package com.hang.curator.basic;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class CuratorGetChild {
    String IP = "192.168.159.129:2181,192.168.159.130:2181,192.168.159.131:2181";
    CuratorFramework client;

    @Before
    public void before() {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.builder()
                .connectString(IP)
                .sessionTimeoutMs(120 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("curator/create")
                .build();
        client.start();
    }

    @After
    public void after() {
        client.close();
    }

    /* ----------------------------------------------------------------------------------------------------------- */

    @Test
    public void getChild1() throws Exception {
        /* 读取子节点的数据 */
        List<String> strings = client.getChildren()
                .forPath("/node3");
        for (String str : strings) {
            System.out.println(str);
        }
    }

    @Test
    public void getChild2() throws Exception {
        /* 异步方式读取子节点 */
        client.getChildren()
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        System.out.println(event.getPath());
                        System.out.println(event.getType());
                        List<String> children = event.getChildren();
                        for (String str : children) {
                            System.out.println(str);
                        }
                    }
                })
                .forPath("/node3");
        Thread.sleep(5000);
        System.out.println("结束");
    }
}
