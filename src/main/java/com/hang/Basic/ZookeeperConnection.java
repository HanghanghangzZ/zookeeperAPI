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
        /**
         * 要创建ZooKeeper客户端对象，应用程序需要传递一个包含以逗号分隔的host：port对列表的连接字符串，每个对对应于ZooKeeper服务器。
         * 会话建立是异步的。 此构造函数将启动与服务器的连接并立即返回-可能（通常）在完全建立会话之前返回。 watcher参数指定将通知状态变化的观察者。 该通知可以在构造函数调用返回之前或之后的任何时间发出。
         * 实例化的ZooKeeper客户端对象将从connectString中选择一个任意服务器，然后尝试连接到该服务器。 如果建立连接失败，将尝试使用连接字符串中的另一台服务器（顺序是不确定的，因为我们随机地随机排列列表），直到建立连接为止。 客户端将继续尝试，直到明确关闭会话为止。
         */
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
