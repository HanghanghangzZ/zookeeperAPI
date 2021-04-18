package com.hang.curator.basic;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CuratorDelete {
    String IP = "192.168.159.129:2181,192.168.159.130:2181,192.168.159.131:2181";
    CuratorFramework client;

    @Before
    public void before() {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.builder()
                .connectString(IP)
                .sessionTimeoutMs(120 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("curator/delete")
                .build();
        client.start();
    }

    @After
    public void after() {
        client.close();
    }

    /* ----------------------------------------------------------------------------------------------------------- */

    @Test
    public void delete1() throws Exception {
        /* 删除节点 */
        client.delete()
                .forPath("/node1");
        System.out.println("结束");
    }

    @Test
    public void delete2() throws Exception {
        client.delete()
                .withVersion(0)         //版本号
                .forPath("/node1");
        System.out.println("结束");
    }

    @Test
    public void delete3() throws Exception {
        /* 删除包含子节点的节点 */
        client.delete()
                .deletingChildrenIfNeeded()
                .withVersion(-1)
                .forPath("/node1");
        System.out.println("结束");
    }

    @Test
    public void delete4() throws Exception {
        /* 异步方式创建节点 */
        client.delete()
                .deletingChildrenIfNeeded()
                .withVersion(-1)
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        System.out.println(event.getPath());
                        System.out.println(event.getType());
//                        System.out.println(new String(event.getData()));
                        /* 在这里执行会报错，但是在getData的回调函数里不会报错 */
                    }
                })
                .forPath("/node1");
        Thread.sleep(5000);
        System.out.println("结束");
    }
}
