package com.hang.Basic;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZookeeperConnection {
    public static void main(String[] args) throws IOException, InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        /* arg1:服务器的ip和端口号 */
        /* arg2：客户端与服务器之间的会话超时时间，以毫秒为单位 */
        /* arg3：监视器对象 */
        ZooKeeper zookeeper = new ZooKeeper("192.168.159.129:2181,192.168.159.130:2181,192.168.159.131:2181", 5000, new Watcher() {
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

        System.out.println(zookeeper.getSessionId());
        zookeeper.close();
    }
}
