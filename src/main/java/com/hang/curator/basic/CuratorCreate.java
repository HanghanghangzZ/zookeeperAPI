package com.hang.curator.basic;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jnlp.ClipboardService;
import java.util.ArrayList;

public class CuratorCreate {
    String IP = "192.168.159.129:2181,192.168.159.130:2181,192.168.159.131:2181";
    CuratorFramework client;

    @Before
    public void before() {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.builder()
                .connectString(IP)
                .sessionTimeoutMs(120 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("curator/create")
                .build();
        client.start();
    }

    @After
    public void after() {
        client.close();
    }

    /* ----------------------------------------------------------------------------------------------------------- */

    @Test
    public void create1() throws Exception {
        String s = client.create()
                .withMode(CreateMode.PERSISTENT)                   //节点的类型
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)               //节点的权限列表
                .forPath("/node5", "node1".getBytes());//arg1: 节点的路径， arg2: 节点的数据 返回新创建节点的path
        System.out.println(s);  //输出新创建节点的path
        System.out.println("结束");
    }

    @Test
    public void create2() throws Exception {
        /* 自定义权限列表 */
        /* 权限列表 */
        ArrayList<ACL> acls = new ArrayList<>();
        /* 授权模式和授权对象 */
        Id id = new Id("ip", "192.168.159.130");
        acls.add(new ACL(ZooDefs.Perms.ALL, id));

        client.create()
                .withMode(CreateMode.PERSISTENT)
                .withACL(acls)
                .forPath("/node2", "node2".getBytes());
        System.out.println("结束");
    }

    @Test
    public void create3() throws Exception {
        client.create()
                .creatingParentsIfNeeded()     //递归节点的创建
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath("/node3/node31", "node31".getBytes());
        System.out.println("结束");
    }

    @Test
    public void create4() throws Exception {
        /* 异步方式创建节点 */
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .inBackground(new BackgroundCallback() {        //异步回调接口
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        /* 节点的路径 */
                        System.out.println(event.getPath());
                        /* 事件类型 */
                        System.out.println(event.getType());
                    }
                })
                .forPath("/node4", "node4".getBytes());
        Thread.sleep(5000);
        System.out.println("结束");
    }
}
