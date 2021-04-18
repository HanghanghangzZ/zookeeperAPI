package com.hang.zookeeperJavaAPI.Basic;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class ZKCreate {
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
    public void  create1() throws InterruptedException, KeeperException {
        /* arg1:节点的路径 */
        /* arg2:节点的数据 */
        /* arg3:权限列表 OPEN_ACL_UNSAFE 表示 world:anyone:crdwa */
        /* arg4:节点类型 持久化节点 */
        zookeeper.create("/create/node1", "node1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void create2() throws InterruptedException, KeeperException {
        /* READ_ACL_UNSAFE 表示 world:anyone:r */
        zookeeper.create("/create/node2", "node2".getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void create3() throws InterruptedException, KeeperException {
        /* 权限列表 */
        ArrayList<ACL> acls = new ArrayList<>();

        /* 授权模式和授权对象 */
        Id id = new Id("world", "anyone");

        /* 权限设置 */
        acls.add(new ACL(ZooDefs.Perms.READ, id));
        acls.add(new ACL(ZooDefs.Perms.WRITE, id));
        zookeeper.create("/create/node3", "node3".getBytes(), acls, CreateMode.PERSISTENT);
    }

    @Test
    public void create4() throws InterruptedException, KeeperException {
        /* 权限列表 */
        ArrayList<ACL> acls = new ArrayList<>();

        Id id = new Id("ip", "192.168.159.130");

        acls.add(new ACL(ZooDefs.Perms.ALL, id));
        zookeeper.create("/create/node4", "node4".getBytes(), acls, CreateMode.PERSISTENT);
    }

    @Test
    public void create5() throws InterruptedException, KeeperException {
        /* auth授权模式 */
        /* 添加授权用户 */
        zookeeper.addAuthInfo("digest", "hang:123456".getBytes());
        /* 这里会发生一件有趣的事，在leader中授权用户成功的被添加了，但是并没有添加进follower */
        /* 猜想：zookeeper的API在对zk集群操作时都是对leader的操作??? */
        zookeeper.create("/create/node5", "node5".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
    }

    @Test
    public void create6() throws InterruptedException, KeeperException {
        /* auth授权模式 */
        /* 添加授权用户 */
        zookeeper.addAuthInfo("digest", "hang:123456".getBytes());
        /* 权限列表 */
        ArrayList<ACL> acls = new ArrayList<>();
        /* 授权模式和授权对象 */
        /* 原来对于auth这种授权模式(scheme)来说，用户名就是它的授权对象(id) */
        Id id = new Id("auth", "hang");
        /* 权限设置 */
        acls.add(new ACL(ZooDefs.Perms.READ, id));
        zookeeper.create("/create/node6", "node6".getBytes(), acls, CreateMode.PERSISTENT);
    }

    @Test
    public void create7() throws InterruptedException, KeeperException {
        /* digest 授权模式 */
        ArrayList<ACL> acls = new ArrayList<>();
        /* 授权模式和授权对象 */
        /* 原来对于digest这种授权模式(scheme)来说，用户名:加密后的密文密码就是它的id */
        Id digest = new Id("digest", "hang:B48MGLbxffZLp8dSCGq780OeB4M=");
        acls.add(new ACL(ZooDefs.Perms.ALL, digest));
        zookeeper.create("/create/node7", "node7".getBytes(), acls, CreateMode.PERSISTENT);
    }

    @Test
    public void create8() throws InterruptedException, KeeperException {
        /* 持久化顺序节点 */
        String s = zookeeper.create("/create/node8", "nodee8".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println(s);
    }

    @Test
    public void create9() throws InterruptedException, KeeperException {
        /* 临时节点 */
        String s = zookeeper.create("/create/node9", "node9".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println(s);
    }

    @Test
    public void create10() throws InterruptedException, KeeperException {
        /* 临时顺序节点 */
        String s = zookeeper.create("/create/node10", "node10".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(s);
    }

    @Test
    public void create11() throws InterruptedException {
        /* 异步方式创建节点 */
        zookeeper.create("/create/node11", "node11".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                /* 0代表创建成功 */
                System.out.println(rc);
                /* 节点的路径 */
                System.out.println(path);
                /* 节点的路径 */
                System.out.println(name);
                /* 上下文参数 */
                System.out.println(ctx);
            }
        }, "I an context");
        Thread.sleep(10000);
        System.out.println("结束");
    }
}












