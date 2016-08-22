package org.shisoft.neb;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by shisoft on 22/8/2016.
 */
public class CellLock {
    private long hash;
    private AtomicInteger acquired;
    private ReentrantReadWriteLock lock;

    public ReentrantReadWriteLock getLock() {
        return lock;
    }

    public long getHash() {
        return hash;
    }

    public AtomicInteger getAcquired() {
        return acquired;
    }

    public CellLock(long hash) {
        this.hash = hash;
        this.lock = new ReentrantReadWriteLock();
        this.acquired = new AtomicInteger(0);
    }
}
