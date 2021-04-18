package com.hang.zookeeperJavaAPI.Watcher;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 工作中有这样的一个场景:
 * 数据库用户名和密码信息放在一个配置文件中，应用 读取该配置文件，配置文件信息放入缓存。
 * 若数据库的用户名和密码改变时候，还需要重新加载缓存，比较麻烦，通过 ZooKeeper可以轻松完成，当数据库发生变化时自动完成缓存同步。
 *
 * 设计思路：
 * 1. 连接zookeeper服务器
 * 2. 读取zookeeper中的配置信息，注册watcher监听器，存入本地变量
 * 3. 当zookeeper中的配置信息发生变化时，通过watcher的回调方法捕获数据变化事件
 * 4. 重新获取配置信息
 */
public class MyConfigCenter implements Watcher {
    // zk的连接串
    String IP = "192.168.159.129:2181,192.168.159.130:2181,192.168.159.131:2181";
    // 计数器对象
    CountDownLatch countDownLatch = new CountDownLatch(1);
    // 连接对象
    static ZooKeeper zooKeeper;
    // 用于本地化存储配置信息
    private String url;
    private String username;
    private String password;

    @Override
    public void process(WatchedEvent event) {
        try {
            // 捕获事件状态
            if (event.getType() == Event.EventType.None) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("连接成功");
                    countDownLatch.countDown();
                } else if (event.getState() == Event.KeeperState.Disconnected) {
                    System.out.println("连接断开!");
                } else if (event.getState() == Event.KeeperState.Expired) {
                    System.out.println("连接超时!");
                    // 超时后服务器端已经将连接释放，需要重新连接服务器端
                    zooKeeper = new ZooKeeper(IP, 6000, this);
                } else if (event.getState() == Event.KeeperState.AuthFailed) {
                    System.out.println("验证失败!");
                }
                // 当配置信息发生变化时
            } else if (event.getType() == Event.EventType.NodeDataChanged) {
                initValue();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // 构造方法
    public MyConfigCenter() throws IOException, InterruptedException, KeeperException {
        // 创建连接对象
        zooKeeper = new ZooKeeper(IP, 5000, this);
        // 阻塞线程，等待连接的创建成功
        countDownLatch.await();
        initValue();
    }

    // 连接zookeeper服务器，读取配置信息
    public void initValue() throws InterruptedException, KeeperException {
        // 读取配置信息
        this.url = new String(zooKeeper.getData("/config/url", true, null));
        this.username = new String(zooKeeper.getData("/config/username", true, null));
        this.password = new String(zooKeeper.getData("/config/password", true, null));
    }

    public static void main(String[] args) throws InterruptedException {
        try {
            MyConfigCenter myConfigCenter = new MyConfigCenter();
            while (true) {
                Thread.sleep(5000);
                System.out.println("url:" + myConfigCenter.getUrl());
                System.out.println("username:" + myConfigCenter.getUsername());
                System.out.println("password:" + myConfigCenter.getPassword());
                System.out.println("########################################");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
