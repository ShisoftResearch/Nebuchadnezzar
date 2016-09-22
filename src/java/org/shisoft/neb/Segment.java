package org.shisoft.neb;

import org.shisoft.neb.io.Reader;

import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by shisoft on 16-4-11.
 */
public class Segment {

    Trunk trunk;
    private long baseAddr;
    private int id;
    private AtomicLong currentLoc;
    private AtomicInteger deadObjectBytes;
    private ReentrantReadWriteLock lock;
    private TreeSet<Long> frags;
    private volatile boolean isDirty;

    public Segment(int id, long baseAddr, Trunk trunk) {
        assert baseAddr >= trunk.getStoreAddress();
        this.id = id;
        this.baseAddr = baseAddr;
        this.trunk = trunk;
        this.currentLoc = new AtomicLong(baseAddr);
        this.deadObjectBytes = new AtomicInteger(0);
        this.lock = new ReentrantReadWriteLock(true);
        this.frags = new TreeSet<>();
        this.isDirty = false;
    }

    public ReentrantReadWriteLock getLock() {
        return lock;
    }

    public int getId() {
        return id;
    }

    public long getBaseAddr() {
        return baseAddr;
    }

    public long getCurrentLoc() {
        return currentLoc.get();
    }

    public boolean resetCurrentLoc (long expected, long update) {
        return currentLoc.compareAndSet(expected, update);
    }

    public int getDeadObjectBytes() {
        return deadObjectBytes.get();
    }

    public int incDeadObjectBytes (int len) {
        return deadObjectBytes.addAndGet(len);
    }

    public int decDeadObjectBytes (int len) {
        return deadObjectBytes.addAndGet(-1 * len);
    }

    public float aliveDataRatio () {
        return 1 - ((float) getDeadObjectBytes()) / ((float) this.currentLoc.get());
    }

    public long getAliveObjectBytes () {
        return currentLoc.get() - baseAddr - deadObjectBytes.get();
    }

    public void setDirty () {this.isDirty = true;}

    public void setClean () {
        this.isDirty = false;
    }

    public boolean isDirty () {return this.isDirty;}

    public TreeSet<Long> getFrags() {
        return frags;
    }

    public void lockWrite () {
        this.lock.writeLock().lock();
    }
    public void unlockWrite () {
        this.lock.writeLock().unlock();
    }

    public void lockRead () {
        this.lock.readLock().lock();
    }
    public void unlockRead () {
        this.lock.readLock().unlock();
    }

    public long tryAcquireSpace (long len) {
        assert len > 0;
        AtomicBoolean updated = new AtomicBoolean(false);
        long r = this.currentLoc.getAndUpdate(originalLoc -> {
            long expectedLoc = originalLoc + len;
            long expectedPos = expectedLoc - baseAddr;
            if (expectedPos >= Trunk.getSegSize()) {
                updated.set(false);
                return originalLoc;
            } else {
                updated.set(true);
                return expectedLoc;
            }
        });
        if (updated.get()) {
            return r;
        } else {
            return -1;
        }
    }

    public byte[] getData () {
        return Reader.readBytes(this.baseAddr, Trunk.getSegSize());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Segment segment = (Segment) o;

        return baseAddr == segment.baseAddr;

    }

    @Override
    public int hashCode() {
        return (int) (baseAddr ^ (baseAddr >>> 32));
    }
}
