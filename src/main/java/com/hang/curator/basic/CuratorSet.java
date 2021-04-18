package com.hang.curator.basic;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CuratorSet {
    String IP = "192.168.159.129:2181,192.168.159.130:2181,192.168.159.131:2181";
    CuratorFramework client;

    @Before
    public void before() {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.builder()
                .connectString(IP)
                .sessionTimeoutMs(120 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("curator/set")
                .build();
        client.start();
    }

    @After
    public void after() {
        client.close();
    }

    /* ----------------------------------------------------------------------------------------------------------- */

    @Test
    public void set1() throws Exception {
        /* 更新节点 */
        client.setData()
                .forPath("/node1", "node11".getBytes());
        System.out.println("结束");
    }

    @Test
    public void set2() throws Exception {
        Stat stat = client.setData()
                .withVersion(1)     //指定版本号
                .forPath("/node1", "node111".getBytes());
        System.out.println(stat);
        System.out.println("结束");
    }

    @Test
    public void set3() throws Exception {
        /* 异步方式修改节点数据 */
        client.setData()
                .withVersion(-1)
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        System.out.println(event.getPath());
                        System.out.println(event.getType());
                    }
                })
                .forPath("/node1", "node1".getBytes());
        Thread.sleep(5000);
        System.out.println("结束");
    }
}
