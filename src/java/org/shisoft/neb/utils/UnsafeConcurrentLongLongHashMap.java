package org.shisoft.neb.utils;

import org.shisoft.neb.io.type_lengths;
import sun.misc.Unsafe;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

/**
 * Created by shisoft on 8/8/2016.
 */

// Concurrent hash map dedicated to cell index

public class UnsafeConcurrentLongLongHashMap {


    private static final int LONG_LEN = type_lengths.longLen;
    private static final int INITIAL_BUCKETS = 1024; // Must be power of 2
    private static final int LOCKS = 32;
    private static final Unsafe u = UnsafeUtils.unsafe;
    private static final int KEY_LOC = 0;
    private static final int VAL_LOC = LONG_LEN;
    private static final int NXT_LOC = LONG_LEN * 2;


    private StampedLock[] locks;
    private StampedLock masterLock;
    private AtomicLong size;
    private volatile long bucketsIndex;
    private volatile long buckets;
    private volatile float bucketsT;

    private void setBuckets (long buckets) {
        this.buckets = buckets;
        this.bucketsT = 1 / buckets;
    }

    private long[] writeLockAll() {
        long[] stamps = new long[locks.length];
        for (int i = 0; i < locks.length; i++) {
            stamps[i] = locks[i].writeLock();
        }
        return stamps;
    }

    private void writeUnlockAll(long[] stamps){
        assert stamps.length == locks.length;
        for (int i = 0; i < stamps.length; i++) {
            locks[i].unlockWrite(stamps[i]);
        }
    }

    private float getAvgBucketItems () {
        return size.longValue() * bucketsT;
    }

    private void checkResize() {
        if (getAvgBucketItems() > 10) {
            long masterStamp = masterLock.writeLock();
            if (getAvgBucketItems() < 10) return; // recheck for lock
            try {
                long originalBucketsIndex = bucketsIndex;
                long originalBuckets = buckets;
                long originalBucketsSize = originalBuckets * LONG_LEN;
                long originalBucketsEnds = originalBucketsIndex + originalBucketsSize;
                buckets = buckets * 2;
                initBuckets();
                for (long bucketAddr = originalBucketsIndex; bucketAddr < originalBucketsEnds; bucketsIndex += LONG_LEN) {
                    long cellLoc = u.getLong(bucketAddr);
                    while (true) {
                        long next = u.getLong(cellLoc + NXT_LOC);
                        long k = u.getLong(cellLoc + KEY_LOC);
                        long v = u.getLong(cellLoc + VAL_LOC);
                        put(k, v);
                        u.freeMemory(cellLoc);
                        if (next < 0) {
                            break;
                        }
                        cellLoc = next;
                    }
                }
                u.freeMemory(originalBucketsIndex);
            } finally {
                masterLock.unlockWrite(masterStamp);
            }
        }
    }

    private void initBuckets(){
        long bucketsSize = buckets * LONG_LEN;
        long bucketsEnds = bucketsIndex + bucketsSize;
        bucketsIndex = u.allocateMemory(bucketsSize);
        int li = 0;
        for (long i = bucketsIndex; i < bucketsEnds; i += LONG_LEN) {
            u.putLong(i, -1L);
            locks[li]= new StampedLock();
            li++;
        }
    }

    private long getFromBucket(long addr, long k) {
        while (true) {
            long key = u.getLong(addr + KEY_LOC);
            if (key == k) {
                return u.getLong(addr + VAL_LOC);
            } else {
                addr = u.getLong(addr + NXT_LOC);
                if (addr < 0) {
                    return -1;
                }
            }
        }
    }

    private long writeBucket (long addr, long k, long v) {
        long head = u.allocateMemory(LONG_LEN * 3);
        u.putLong(addr + KEY_LOC, k);
        u.putLong(addr + VAL_LOC, v);
        u.putLong(addr + NXT_LOC, addr);
        size.incrementAndGet();
        return head;
    }

    private long removeBucketCell(long root, long parent, long addr) {
        long next = u.getLong(addr + NXT_LOC);
        if (parent > 0) {
            u.putLong(parent + NXT_LOC, next);
        }
        u.freeMemory(addr);
        size.decrementAndGet();
        return root == addr ? next : root;
    }

    private long removeBucketCellByKey (long addr, long k) {
        long parent = -1;
        long root = addr;
        while (true) {
            long key = u.getLong(addr + KEY_LOC);
            if (key == k) {
                return removeBucketCell(root, parent, addr);
            } else {
                addr = u.getLong(addr + NXT_LOC);
                if (addr < 0) {
                    return -1;
                }
            }
            parent = addr;
        }
    }

    private long setBucketByKeyValue (long addr, long k, long v) {
        long root = addr;
        while (true) {
            long key = u.getLong(addr + KEY_LOC);
            if (key == k) {
                u.putLong(addr + VAL_LOC, v);
                return root;
            } else {
                addr = u.getLong(addr + NXT_LOC);
                if (addr < 0) {
                    break;
                }
            }
        }
        return writeBucket(root, k, v);
    }

    private boolean containsBucketKey (long addr, long k) {
        while (true) {
            long key = u.getLong(addr + KEY_LOC);
            if (key == k) {
                return true;
            } else {
                addr = u.getLong(addr + NXT_LOC);
                if (addr < 0) {
                    return false;
                }
            }
        }
    }

    private int locateBuckeyByKey(long key) {
        return (int) (key & (buckets - 1));
    }

    private StampedLock locateLockByBucketId(int id) {
        return locks[id & (LOCKS - 1)];
    }

    public long getBucketHeaderByID (int bucketId) {
        return buckets + bucketId * LONG_LEN;
    }

    public long getBucketsHeaderByKey (long key) {
        return getBucketHeaderByID(locateBuckeyByKey(key));
    }

    public UnsafeConcurrentLongLongHashMap() {
        this.locks = new StampedLock[LOCKS];
        this.size = new AtomicLong();
        this.masterLock = new StampedLock();
        setBuckets(INITIAL_BUCKETS);
        initBuckets();
    }

    public long size() {
        return size.longValue();
    }


    public boolean isEmpty() {
        return size.longValue() == 0;
    }

    public boolean containsKey(long key) {
        return containsBucketKey(getBucketsHeaderByKey(key), key);
    }

    public long get(long key) {
        long masterStamp = masterLock.readLock();
        int bucketId = locateBuckeyByKey(key);
        StampedLock lock = locateLockByBucketId(bucketId);
        long stamp = lock.readLock();
        try {
            return getFromBucket(getBucketsHeaderByKey(key), key);
        } finally {
            lock.unlockRead(stamp);
            masterLock.unlockRead(masterStamp);
            checkResize();
        }
    }

    public long put(long key, long value) {
        long masterStamp = masterLock.readLock();
        int bucketId = locateBuckeyByKey(key);
        StampedLock lock = locateLockByBucketId(bucketId);
        long stamp = lock.writeLock();
        try {
            return setBucketByKeyValue(getBucketsHeaderByKey(key), key, value);
        } finally {
            lock.unlockWrite(stamp);
            masterLock.unlockRead(masterStamp);
            checkResize();
        }
    }

    public Object remove(long key) {
        long masterStamp = masterLock.readLock();
        int bucketId = locateBuckeyByKey(key);
        StampedLock lock = locateLockByBucketId(bucketId);
        long stamp = lock.writeLock();
        try {
            return removeBucketCellByKey(getBucketsHeaderByKey(key), key);
        } finally {
            lock.unlockWrite(stamp);
            masterLock.unlockRead(masterStamp);
            checkResize();
        }
    }

    public void putAll(Map<Long, Long> m) {
        for (Map.Entry<Long, Long> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    public void clear() {
        long masterStamp = masterLock.writeLock();
        long bucketsSize = buckets * LONG_LEN;
        long bucketsEnds = bucketsIndex + bucketsSize;
        try {
            for (long bucketAddr = bucketsIndex; bucketAddr < bucketsEnds; bucketsIndex += LONG_LEN) {
                long cellLoc = u.getLong(bucketAddr);
                while (true) {
                    long next = u.getLong(cellLoc + NXT_LOC);
                    u.freeMemory(cellLoc);
                    if (next < 0) {
                        break;
                    }
                    cellLoc = next;
                }
            }
        } finally {
            masterLock.unlockWrite(masterStamp);
        }
    }

}
