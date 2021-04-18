package com.hang.zookeeperJavaAPI.Basic;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class zkDelete {
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
    public void delete1() throws InterruptedException, KeeperException {
        /* arg1:删除节点的节点路径 */
        /* arg2:数据版本信息 -1代表删除节点时不考虑版本信息 */
        zookeeper.delete("/delete/node1", -1);
    }

    @Test
    public void delete2() throws InterruptedException {
        /* 异步方式 */
        zookeeper.delete("/delete/node2", -1, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                /* 0代表删除成功 */
                System.out.println(rc);
                /* 节点的路径 */
                System.out.println(path);
                /* 上下文参数对象 */
                System.out.println(ctx);
            }
        }, "I am a Context");
        Thread.sleep(5000);
        System.out.println("结束");
    }

}
