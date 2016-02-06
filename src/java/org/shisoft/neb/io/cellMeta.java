package org.shisoft.neb.io;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by shisoft on 5/2/2016.
 */
public class cellMeta {

    ReentrantLock lock = new ReentrantLock();
    volatile long location;

    public cellMeta(long location) {
        this.location = location;
    }

    public long getLocation() {
        return location;
    }

    public void setLocation(long location) {
        this.location = location;
    }

    public void lockWrite(){
        lock.lock();
    }

    public void unlockWrite(){
        lock.unlock();
    }

    public void lockRead(){
        lock.lock();
    }

    public void unlockRead(){
        lock.unlock();
    }
}
