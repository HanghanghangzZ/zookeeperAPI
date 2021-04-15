package com.hang.Watcher;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZKWatcherGetData {
    String IP = "192.168.159.129:2181,192.168.159.130:2181,192.168.159.131:2181";
    ZooKeeper zookeeper;

    @Before
    public void before() throws InterruptedException, IOException {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        /* arg1:服务器的ip和端口号 */
        /* arg2：客户端与服务器之间的会话超时时间，以毫秒为单位 */
        /* arg3：监视器对象 */
        zookeeper = new ZooKeeper(IP, 5000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("连接创建成功");
                    countDownLatch.countDown();
                }
            }
        });
        /* 主线程阻塞等待客户端连接对象的创建成功 */
        countDownLatch.await();
    }

    @After
    public void after() throws InterruptedException {
        zookeeper.close();
    }

    /* -------------------------------------------------------------------------------------------------- */

    @Test
    public void watcherGetData1() throws InterruptedException, KeeperException {
        /* arg1:节点的路径 */
        /* arg2:使用连接对象中的watcher */
        /* 如果监视为真且调用成功（不会引发异常），则监视将留在具有给定路径的节点上。 成功设置该节点上的数据或删除该节点的操作将触发监视。 */
        zookeeper.getData("/watcher2", true, null);
        Thread.sleep(50000);
        System.out.println("结束");
    }

    @Test
    public void watcherGetData2() throws InterruptedException, KeeperException {
        /* arg1:节点的路径 */
        /* arg2:自定义watcher对象 */
        zookeeper.getData("/watcher2", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("自定义watcher");
                System.out.println("path = " + event.getPath());
                System.out.println("eventType = " + event.getType());
            }
        }, null);
        Thread.sleep(50000);
        System.out.println("结束");
    }

    @Test
    public void watcherGetData3() throws KeeperException, InterruptedException {
        // 一次性
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    System.out.println("自定义watcher");
                    System.out.println("path=" + event.getPath());
                    System.out.println("eventType=" + event.getType());
                    /* getData只会监听NodeDeleted和NodeDataChanged两种事件 */
                    /* 如果触发的不是NodeDataChanged，那就表明该节点已经被删除了 */
                    /* 那么getData是没有办法再给这个节点添加监听器的 */
                    if (event.getType() == Event.EventType.NodeDataChanged) {
                        zookeeper.getData("/watcher2", this, null);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };
        zookeeper.getData("/watcher2", watcher, null);
        Thread.sleep(50000);
        System.out.println("结束");
    }

    @Test
    public void watcherGetData4() throws KeeperException, InterruptedException {
        // 注册多个监听器对象
        zookeeper.getData("/watcher2", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    System.out.println("1");
                    System.out.println("path=" + event.getPath());
                    System.out.println("eventType=" + event.getType());
                    if (event.getType() == Event.EventType.NodeDataChanged) {
                        zookeeper.getData("/watcher2", this, null);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }, null);
        zookeeper.getData("/watcher2", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    System.out.println("2");
                    System.out.println("path=" + event.getPath());
                    System.out.println("eventType=" + event.getType());
                    if (event.getType() == Event.EventType.NodeDataChanged) {
                        zookeeper.getData("/watcher2", this, null);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }, null);
        Thread.sleep(50000);
        System.out.println("结束");
    }
}

