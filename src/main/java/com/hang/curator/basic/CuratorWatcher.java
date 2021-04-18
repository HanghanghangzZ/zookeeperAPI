package com.hang.curator.basic;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class CuratorWatcher {
    String IP = "192.168.159.129:2181,192.168.159.130:2181,192.168.159.131:2181";
    CuratorFramework client;

    @Before
    public void before() {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.builder()
                .connectString(IP)
                .sessionTimeoutMs(120 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("curator/watcher")
                .build();
        client.start();
    }

    @After
    public void after() {
        client.close();
    }

    /* ----------------------------------------------------------------------------------------------------------- */

    @Test
    public void watcher1() throws InterruptedException {
        /* 在event回调函数中，oldData表示之前的数据，data表示更新后的数据。并且对子节点无限制深度的监听，还可以对自己的监听 */
        CountDownLatch countDownLatch = new CountDownLatch(1);
        CuratorCache curatorCache = CuratorCache.build(client, "/watcher1");
        curatorCache.start();
        curatorCache.listenable().addListener(new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                System.out.println("-------------type---------------");
                System.out.println(type.name());

                System.out.println("--------------old----------------");
                System.out.println(oldData.getPath());
                System.out.println(new String(oldData.getData(), StandardCharsets.UTF_8));

                System.out.println("--------------new----------------");
                System.out.println(data.getPath());
                System.out.println(new String(data.getData(), StandardCharsets.UTF_8));
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
        System.out.println("结束");
        /* 关闭监视器对象 */
        curatorCache.close();
    }
}