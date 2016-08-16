package org.shisoft.neb.utils.unsafe.map;

import org.shisoft.neb.io.type_lengths;
import org.shisoft.neb.utils.UnsafeUtils;
import sun.misc.Unsafe;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

/**
 * Created by shisoft on 8/8/2016.
 */

// Concurrent hash map dedicated to cell index

public class UnsafeConcurrentLongLongHashMap {


    private static final int INITIAL_BUCKETS = 1024; // Must be power of 2
    private static final int MAXIMUM_BUCKETS = INITIAL_BUCKETS * 102400; // 800 MB total, must be power of 2 either

    private static final int LONG_LEN = type_lengths.longLen;
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
        this.bucketsT = 1 / (float) buckets;
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
        return (float) size.longValue() * bucketsT;
    }

    public boolean resizeRequired() {
        return getAvgBucketItems() > 10 && buckets < MAXIMUM_BUCKETS;
    }

    private void checkResize() {
        if (resizeRequired()) {
            long masterStamp = masterLock.writeLock();
            try {
                if (!resizeRequired()) return; // recheck for lock
                long originalBucketsIndex = bucketsIndex;
                long originalBuckets = buckets;
                long originalBucketsSize = originalBuckets * LONG_LEN;
                long originalBucketsEnds = originalBucketsIndex + originalBucketsSize;
                buckets = buckets * 2;
                initBuckets();
                for (long bucketAddr = originalBucketsIndex; bucketAddr < originalBucketsEnds; bucketAddr += LONG_LEN) {
                    long cellLoc = u.getLong(bucketAddr);
                    while (true) {
                        if (cellLoc < 0) break;
                        long next = u.getLong(cellLoc + NXT_LOC);
                        long k = u.getLong(cellLoc + KEY_LOC);
                        long v = u.getLong(cellLoc + VAL_LOC);
                        put_lockfree(k, v);
                        u.freeMemory(cellLoc);
                        cellLoc = next;
                    }
                }
                u.freeMemory(originalBucketsIndex);
            } finally {
                masterLock.unlockWrite(masterStamp);
            }
        }
    }

    private long getFromBucket(long bucketLoc, long k) {
        long addr = u.getLong(bucketLoc);
        if (addr < 0) return  -1;
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
        u.putLong(head + KEY_LOC, k);
        u.putLong(head + VAL_LOC, v);
        u.putLong(head + NXT_LOC, addr);
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

    private long removeBucketCellByKey (long bucketLoc, long k) {
        long addr = u.getLong(bucketLoc);
        long parent = -1;
        long root = addr;
        while (true) {
            if (addr > 0) {
                long key = u.getLong(addr + KEY_LOC);
                if (key == k) {
                    long newHeader = removeBucketCell(root, parent, addr);
                    u.putLong(bucketLoc, newHeader);
                    return newHeader;
                }
                addr = u.getLong(addr + NXT_LOC);
            }
            if (addr < 0) {
                return -1;
            }
            parent = addr;
        }
    }

    private long setBucketByKeyValue (long bucketLoc, long k, long v) {
        long addr = u.getLong(bucketLoc);
        long root = addr;
        while (true) {
            if (addr > 0) {
                long key = u.getLong(addr + KEY_LOC);
                if (key == k) {
                    u.putLong(addr + VAL_LOC, v);
                    return root;
                }
                addr = u.getLong(addr + NXT_LOC);
            }
            if (addr < 0) {
                break;
            }
        }
        long newHeader = writeBucket(root, k, v);
        u.putLong(bucketLoc, newHeader);
        return newHeader;
    }

    private boolean containsBucketKey (long bucketLoc, long k) {
        long addr = u.getLong(bucketLoc);
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

    private long getBucketLocationByID(int bucketId) {
        return bucketsIndex + bucketId * LONG_LEN;
    }

    private long getBucketsLocationByKey(long key) {
        return getBucketLocationByID(locateBuckeyByKey(key));
    }

    public UnsafeConcurrentLongLongHashMap() {
        this.locks = new StampedLock[LOCKS];
        this.size = new AtomicLong();
        this.masterLock = new StampedLock();
        setBuckets(INITIAL_BUCKETS);
        initBuckets();

        for (int li = 0; li < LOCKS; li++) {
            locks[li]= new StampedLock();
        }
    }

    public long size() {
        return size.longValue();
    }


    public boolean isEmpty() {
        return size.longValue() == 0;
    }

    public boolean containsKey(long key) {
        return containsBucketKey(getBucketsLocationByKey(key), key);
    }

    public long get(long key) {
        long masterStamp = masterLock.readLock();
        int bucketId = locateBuckeyByKey(key);
        StampedLock lock = locateLockByBucketId(bucketId);
        long stamp = lock.readLock();
        try {
            return getFromBucket(getBucketLocationByID(bucketId), key);
        } finally {
            lock.unlockRead(stamp);
            masterLock.unlockRead(masterStamp);
            checkResize();
        }
    }

    public  long put_lockfree(long key, long value){
        long bucketLoc = getBucketLocationByID(locateBuckeyByKey(key));
        return setBucketByKeyValue(bucketLoc, key, value);
    }

    public long put_(long key, long value) {
        int bucketId = locateBuckeyByKey(key);
        StampedLock lock = locateLockByBucketId(bucketId);
        long stamp = lock.writeLock();
        try {
            long bucketLoc = getBucketLocationByID(bucketId);
            return setBucketByKeyValue(bucketLoc, key, value);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public long put(long key, long value) {
        long masterStamp = masterLock.readLock();
        try {
            return put_(key, value);
        } finally {
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
            long bucketLoc = getBucketLocationByID(bucketId);
            return removeBucketCellByKey(bucketLoc, key);
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

    private void initBuckets(){
        long bucketsSize = buckets * LONG_LEN;
        bucketsIndex = u.allocateMemory(bucketsSize);
        long bucketsEnds = bucketsIndex + bucketsSize;
        for (long i = bucketsIndex; i < bucketsEnds; i += LONG_LEN) {
            u.putLong(i, -1L);
        }
    }

    private void clear_(){
        long bucketsSize = buckets * LONG_LEN;
        long bucketsEnds = bucketsIndex + bucketsSize;
        for (long i = bucketsIndex; i < bucketsEnds; i += LONG_LEN) {
            long cellLoc = u.getLong(i);
            if (cellLoc < 0) continue;
            while (true) {
                if (cellLoc < 0) break;
                long next = u.getLong(cellLoc + NXT_LOC);
                u.freeMemory(cellLoc);
                cellLoc = next;
            }
            u.putLong(i, -1L);
        }
        size.set(0);
    }

    public void clear() {
        long masterStamp = masterLock.writeLock();
        try {
            clear_();
        } finally {
            masterLock.unlockWrite(masterStamp);
        }
    }

    public void dispose(){
        long masterStamp = masterLock.writeLock();
        try {
            clear_();
            u.freeMemory(bucketsIndex);
        } finally {
            masterLock.unlockWrite(masterStamp);
        }
    }

    public long getBuckets() {
        return buckets;
    }

    public long getInitialBuckets () {
        return INITIAL_BUCKETS;
    }
}
