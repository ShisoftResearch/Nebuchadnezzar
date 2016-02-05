package org.shisoft.neb.io;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by shisoft on 5/2/2016.
 */
public class cellMeta {

    ReentrantLock lock = new ReentrantLock();
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
