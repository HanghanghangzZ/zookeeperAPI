package com.hang.curator.basic;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * InterProcessMutex：分布式可重入排它锁
 * InterProcessReadWriteLock：分布式读写锁
 */
public class CuratorLock {
    String IP = "192.168.159.129:2181,192.168.159.130:2181,192.168.159.131:2181";
    CuratorFramework client1;
    CuratorFramework client2;

    @Before
    public void before() {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client1 = CuratorFrameworkFactory.builder()
                .connectString(IP)
                .sessionTimeoutMs(120 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("curator/lock")
                .build();
        client1.start();

        client2 = CuratorFrameworkFactory.builder()
                .connectString(IP)
                .sessionTimeoutMs(120 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("curator/lock")
                .build();
        client2.start();
    }

    @After
    public void after() {
        client1.close();
        client2.close();
    }

    /* ----------------------------------------------------------------------------------------------------------- */

    public void IPMLock1(InterProcessMutex lock) throws Exception {
        lock.acquire();
        System.out.println("lock1成功获取锁");
        /* 可重入锁 */
        IPMLock2(lock);
        lock.release();
        System.out.println("lock1成功释放锁");
    }

    public void IPMLock2(InterProcessMutex lock) throws Exception {
        lock.acquire();
        System.out.println("lock2成功获取锁");
        Thread.sleep(1000 * 5);

        lock.release();
        System.out.println("lock2成功释放锁");
    }

    public void IPMLock3(InterProcessMutex lock) throws Exception {
        lock.acquire();
        System.out.println("lock3成功获取锁");
        lock.release();
        System.out.println("lock3成功释放锁");
    }

    @Test
    public void lock1() throws Exception {
        // 可重入排他锁
        // arg1:连接对象
        // arg2:节点路径
        InterProcessMutex interProcessMutex = new InterProcessMutex(client1, "/lock1");
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    IPMLock1(interProcessMutex);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
        /* 如果此处的3也是起一个线程，那么就会报错，猜想是排他锁的原因 */
        IPMLock3(interProcessMutex);
    }


    public void RWLock1(InterProcessReadWriteLock lock) {
        /* 获取读锁 */
        InterProcessMutex readLock = lock.readLock();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println(Thread.currentThread().getName() + " 获取读锁");
                    readLock.acquire();
                    Thread.sleep(1000 * 2);
                    readLock.release();
                    System.out.println(Thread.currentThread().getName() + " 释放读锁");

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public void RWLock2(InterProcessReadWriteLock lock) {
        /* 获取写锁 */
        InterProcessMutex writeLock = lock.writeLock();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println(Thread.currentThread().getName() + " 获取写锁");
                    writeLock.acquire();
                    Thread.sleep(1000 * 2);
                    System.out.println(Thread.currentThread().getName() + " 释放写锁");
                    writeLock.release();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    @Test
    public void lock2() throws InterruptedException {
        /* 读写锁 */
        InterProcessReadWriteLock interProcessReadWriteLock = new InterProcessReadWriteLock(client1, "/lock1");

        RWLock1(interProcessReadWriteLock);
        Thread.sleep(30 * 1000);
        RWLock2(interProcessReadWriteLock);
        Thread.sleep(30 * 1000);
    }
}
