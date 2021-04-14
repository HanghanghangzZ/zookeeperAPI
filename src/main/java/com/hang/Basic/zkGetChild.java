package com.hang.Basic;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class zkGetChild {
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

    /* 对应zk命令行中的 ls */

    @Test
    public void get1() throws InterruptedException, KeeperException {
        /* arg1:节点的路径 */
        List<String> children = zookeeper.getChildren("/get", false);
        for (String str : children) {
            System.out.println(str);
        }
    }

    @Test
    public void get2() throws InterruptedException {
        /* 异步方法 */
        zookeeper.getChildren("/get", false, new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                System.out.println(Thread.currentThread().getName());
                System.out.println(rc);

                System.out.println(path);
                System.out.println(ctx);
                System.out.println(children);
            }
        }, "I am a context");
        /* 如果把下面这条sleep去掉，可能会导致主线程比上面执行异步方法的线程先执行完导致与zkServer连接断开而无法正常执行异步方法 */
        Thread.sleep(5000);
        System.out.println("结束");
    }
}
