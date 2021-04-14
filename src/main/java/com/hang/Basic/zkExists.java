package com.hang.Basic;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class zkExists {
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

    /* 对应zk命令行中的stat */

    @Test
    public void exists1() throws InterruptedException, KeeperException {
        /* arg1:节点的路径 */
        Stat exists = zookeeper.exists("/exists1", false);
        System.out.println(exists.getVersion());
    }

    @Test
    public void exists2() throws InterruptedException {
        /* 异步方法 */
        zookeeper.exists("/exists1", false, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                System.out.println(rc);
                System.out.println(path);
                System.out.println(ctx);
                System.out.println(stat);
            }
        }, "I am a Context");
        Thread.sleep(5000);
        System.out.println("结束");
    }
}










