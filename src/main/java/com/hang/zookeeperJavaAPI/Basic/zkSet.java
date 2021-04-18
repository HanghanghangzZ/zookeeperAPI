package com.hang.zookeeperJavaAPI.Basic;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class zkSet {
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
    public void set1() throws InterruptedException, KeeperException {
        /* arg1：节点的路径 */
        /* arg2:节点修改的数据 */
        /* arg3:版本号 -1代表版本号不作为修改条件 */
        /* 如果版本号不正确，会报如下错误 */
        /* org.apache.zookeeper.KeeperException$BadVersionException: KeeperErrorCode = BadVersion for /set/node1 */
        Stat stat = zookeeper.setData("/set/node1", "node11111".getBytes(), -1);
        /* 节点的版本号 */
        System.out.println(stat.getVersion());
        /* 节点的创建时间 */
        System.out.println(stat.getCtime());
    }

    @Test
    public void set2() throws InterruptedException {
        /* 异步方式修改节点 */
        zookeeper.setData("/set/node2", "node2222".getBytes(), -1, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                /* 0代表修改成功 */
                System.out.println(rc);
                /* 节点的路径 */
                System.out.println(path);
                /* 版本信息 */
                System.out.println(stat.getAversion());
                /* 上下文参数 */
                System.out.println(ctx);
            }
        }, "I am Context");
        Thread.sleep(5000);
        System.out.println("结束");
    }
}








