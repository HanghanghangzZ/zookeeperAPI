package com.hang.Watcher;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 *  客服端与服务器的连接状态
 *  KeeperState 通知状态
 *  SyncConnected:客户端与服务器正常连接时
 *  Disconnected:客户端与服务器断开连接时
 *  Expired:会话session失效时
 *  AuthFailed:身份认证失败时 事件类型为:None
 */
public class ZKConnectionWatcher implements Watcher {
    /* 计数器对象 */
    static CountDownLatch countDownLatch = new CountDownLatch(1);
    /* 连接对象 */
    static ZooKeeper zooKeeper;

    @Override
    public void process(WatchedEvent event) {
        try {
            if (event.getType() == Event.EventType.None) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("连接创建成功!");
                    countDownLatch.countDown();
                } else if (event.getState() == Event.KeeperState.Disconnected) {
                    System.out.println("断开连接");
                } else if (event.getState() == Event.KeeperState.Expired) {
                    System.out.println("会话超时");
                } else if (event.getState() == Event.KeeperState.AuthFailed) {
                    System.out.println("认证失效");
                }
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            zooKeeper = new ZooKeeper("192.168.159.129:2181,192.168.159.130:2181,192.168.159.131:2181", 5000, new ZKConnectionWatcher());
            /* 阻塞线程等待连接的创建 */
            countDownLatch.await();
            /* 会话id */
            System.out.println(zooKeeper.getSessionId());

            /* 添加授权用户 */
            zooKeeper.addAuthInfo("digest", "hang:123456".getBytes());
            byte[] data = zooKeeper.getData("/hang", false, null);
            System.out.println(new String(data));
            Thread.sleep(5000);
            zooKeeper.close();
            System.out.println("结束");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}















