package org.shisoft.neb.io;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by shisoft on 5/2/2016.
 */
public class cellLock {

    int hash;
    ReentrantReadWriteLock lock;
    AtomicInteger operationsInProgress;
    ReentrantLock initLock = new ReentrantLock();

    public boolean init(int hash){
        try {
            this.hash = hash;
            initLock.tryLock(1, TimeUnit.MILLISECONDS);
            lock = new ReentrantReadWriteLock();
            operationsInProgress = new AtomicInteger(0);
        } catch (InterruptedException ignored) {
        }
        return true;
    }

    public cellLock lockWrite (){
        operationsInProgress.incrementAndGet();
        lock.writeLock().lock();
        return this;
    }

    public cellLock lockRead (){
        operationsInProgress.incrementAndGet();
        lock.readLock().lock();
        return this;
    }

    public int unlockWrite (){
        lock.writeLock().unlock();
        return operationsInProgress.decrementAndGet();
    }

    public int unlockRead (){
        lock.readLock().unlock();
        return operationsInProgress.decrementAndGet();
    }

    public int getOperationsInProgress (){
        return operationsInProgress.get();
    }

    public int getHash() {
        return hash;
    }
}
