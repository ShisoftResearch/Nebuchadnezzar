package org.shisoft.neb.io;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by shisoft on 5/2/2016.
 */
public class cellMeta {

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    AtomicInteger location;

    public cellMeta(int location) {
        this.location = new AtomicInteger(location);
    }

    public int getLocation() {
        return location.get();
    }

    public void setLocation(int location) {
        this.location.set(location);
    }

    public ReentrantReadWriteLock getLock() {
        return lock;
    }

    public void setLock(ReentrantReadWriteLock lock) {
        this.lock = lock;
    }


    public void lockWrite(){
        lock.writeLock().lock();
    }

    public void unlockWrite(){
        lock.writeLock().unlock();
    }

    public void lockRead(){
        lock.readLock().lock();
    }

    public void unlockRead(){
        lock.readLock().unlock();
    }
}
