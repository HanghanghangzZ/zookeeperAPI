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

public class ZKWatcherExists {
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
    public void watcherExists1() throws InterruptedException, KeeperException {
        /* arg1:节点的路径 */
        /* arg1:连接对象中的watcher */
        /* 如果监视为真且调用成功（不会引发异常），则监视将留在具有给定路径的节点上。 成功创建/删除节点或在节点上设置数据的操作将触发监视。 */
        /* 并且好像只能触发一次 */
        zookeeper.exists("/watcher1", true);
        Thread.sleep(50000);
        System.out.println("结束");
    }

    @Test
    public void watcherExists2() throws InterruptedException, KeeperException {
        /* arg1:节点的路径 */
        /* arg2:自定义watcher对象 */
        /* 这里的watcher也是可以只执行一次 */
        zookeeper.exists("/watcher1", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("自定义watcher");
                System.out.println("path = " + event.getPath());
                System.out.println("eventType = " + event.getType());
            }
        });
        Thread.sleep(50000);
        System.out.println("结束");
    }

    @Test
    public void watcherExists3() throws InterruptedException, KeeperException {
        /* watcher一次性 */
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("自定义watcher");
                System.out.println("path = " + event.getPath());
                System.out.println("eventType = " + event.getType());
                /* watcher只能被触发一次，随后销毁 */
                /* 但是如果我们在watcher中再次对这个节点增加watcher，那么就可以让watcher一直被触发 */
                try {
                    zookeeper.exists("/watcher1", this);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        zookeeper.exists("/watcher1", watcher);
        Thread.sleep(80000);
        System.out.println("结束");
    }

    @Test
    public void watcherExists4() throws InterruptedException, KeeperException {
        /* 尝试注册多个监听器对象 */
        /* 执行后发现，这多个监听器对象是同时触发的 */
        zookeeper.exists("/watcher1", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("1");
                System.out.println("path = " + event.getPath());
                System.out.println("eventType = " + event.getType());
            }
        });
        zookeeper.exists("/watcher1", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("2");
                System.out.println("path = " + event.getPath());
                System.out.println("eventType = " + event.getType());
            }
        });
        Thread.sleep(80000);
        System.out.println("结束");
    }
}
