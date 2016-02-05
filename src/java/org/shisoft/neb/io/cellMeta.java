package org.shisoft.neb.io;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by shisoft on 5/2/2016.
 */
public class cellMeta {

    ReentrantLock lock = new ReentrantLock();
    volatile int location;

    public cellMeta(int location) {
        this.location = location;
    }

    public int getLocation() {
        return location;
    }

    public void setLocation(int location) {
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
