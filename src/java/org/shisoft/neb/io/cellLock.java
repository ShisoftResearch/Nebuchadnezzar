package org.shisoft.neb.io;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by shisoft on 5/2/2016.
 */
public class cellLock {

    ReentrantReadWriteLock lock;
    AtomicInteger operationsInProgress;
    ReentrantLock initLock = new ReentrantLock();

    public boolean init(){
        try {
            initLock.tryLock(1, TimeUnit.MILLISECONDS);
            lock = new ReentrantReadWriteLock();
            operationsInProgress = new AtomicInteger(0);
        } catch (InterruptedException ignored) {
        }
        return true;
    }

    public int lockWrite (){
        int oip = operationsInProgress.incrementAndGet();
        lock.writeLock().lock();
        return oip;
    }

    public int lockRead (){
        int oip = operationsInProgress.incrementAndGet();
        lock.readLock().lock();
        return oip;
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
}
