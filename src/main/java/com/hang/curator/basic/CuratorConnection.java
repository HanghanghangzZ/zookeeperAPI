package com.hang.curator.basic;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.retry.RetryUntilElapsed;

public class CuratorConnection {
    public static void main(String[] args) {
        /* session 重连策略 */
        /* 1. 3秒后重连一次，只重连一次 */
//        RetryOneTime retryOneTime = new RetryOneTime(3000);
        /* 2. 每3秒重连一次，重连3次 */
//        RetryNTimes retryNTimes = new RetryNTimes(3, 3000);
        /* 3. 每3秒重连一次，总等待时间超过10秒后停止重连 */
//        RetryUntilElapsed retryUntilElapsed = new RetryUntilElapsed(10000, 3000);

        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);

        /* 在Curator中CuratorFramework对象就代表一个zookeeper客户端。所以创建创建zookeeper客户端就是创建CuratorFramework对象。 */
        /* CuratorFramework对象又可以通过CuratorFrameworkFactory来创建。*/
        /* 创建连接对象 */
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("192.168.159.129:2181,192.168.159.130:2181,192.168.159.131:2181")    //IP地址端口号
                .sessionTimeoutMs(60 * 1000)         //会话超时时间
                .retryPolicy(retryPolicy)       //重连机制
                .namespace("create")            //命名空间，这个会话的操作都将在这个节点下进行
                .build();                       //构建连接对象

        /* 打开连接 */
        client.start();
        System.out.println(client.getState());
        /* 关闭连接 */
        client.close();
    }
}
