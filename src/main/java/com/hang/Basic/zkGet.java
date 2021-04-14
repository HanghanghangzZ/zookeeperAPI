package com.hang.Basic;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class zkGet {
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
    public void get1() throws InterruptedException, KeeperException {
        /* arg1:节点的路径 */
        /* arg2:是否添加watcher */
        /* arg3:读取节点属性的对象 */
        Stat stat = new Stat();
        byte[] data = zookeeper.getData("/get/node1", false, stat);

        System.out.println(new String(data));
        System.out.println(stat.getVersion());
    }

    @Test
    public void get2() throws InterruptedException {
        /* 异步方式 */
        zookeeper.getData("/get/node1", false, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                /* 0代表读取成功 */
                System.out.println(rc);

                System.out.println(path);
                System.out.println(ctx);
                System.out.println(new String(data));
                System.out.println(stat.getVersion());
            }
        }, "I am a context");
        Thread.sleep(5000);
        System.out.println("结束");
    }
}













