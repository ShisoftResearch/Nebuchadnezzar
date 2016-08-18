package org.shisoft.neb;

import net.openhft.koloboke.collect.map.LongLongMap;
import net.openhft.koloboke.collect.map.hash.HashLongLongMaps;
import org.shisoft.neb.exceptions.ObjectTooLargeException;
import org.shisoft.neb.io.Writer;
import org.shisoft.neb.utils.UnsafeUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.shisoft.neb.io.type_lengths.byteLen;
import static org.shisoft.neb.io.type_lengths.intLen;

/**
 * Created by shisoft on 18/1/2016.
 */
public class Trunk {

    final static int tombstoneSize = intLen + 1;
    final static int segSize    = 8 * 1024 * 1024; //8M Segment
    final static int maxObjSize = 1 * 1024 * 1024; //1M Object
    final static int cellLockCount = 256;

    private int id;
    private long storeAddress;
    private long size;
    private LongLongMap cellIndex = HashLongLongMaps.newMutableMap();
    private boolean backendEnabled = false;
    private Cleaner cleaner;
    private ConcurrentLinkedQueue<Segment> segmentsQueue;
    private Segment[] segments;
    private ReentrantReadWriteLock[] cellLocks;
    public boolean isBackendEnabled() {
        return backendEnabled;
    }
    public ConcurrentLinkedQueue<Segment> getSegmentsQueue() {
        return segmentsQueue;
    }
    public long getSize() {
        return size;
    }
    public LongLongMap getCellIndex() {
        return cellIndex;
    }
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public Cleaner getCleaner() {
        return cleaner;
    }
    public static int getSegSize() {
        return segSize;
    }
    public static int getMaxObjSize() {
        return maxObjSize;
    }
    public Segment[] getSegments() {
        return segments;
    }
    public void setSegments(Segment[] segments) {
        this.segments = segments;
    }

    public Trunk(long size){
        this.size = size;
        storeAddress = getUnsafe().allocateMemory(size);
        cleaner = new Cleaner(this);
        initLocks();
        initSegments(segSize);
    }

    private void initLocks() {
        cellLocks = new ReentrantReadWriteLock[cellLockCount];
        for (int i = 0; i < cellLocks.length; i++) {
            cellLocks[i] = new ReentrantReadWriteLock();
        }
    }

    public ReentrantReadWriteLock locateLock(long hash) {
        return cellLocks[(int) (hash & (cellLockCount - 1))];
    }

    private void initSegments (long segSize) {
        int segCount = (int) Math.floor((double) this.size / segSize);
        assert segCount > 0;
        segmentsQueue = new ConcurrentLinkedQueue<>();
        segments = new Segment[segCount];
        for (int i = 0; i < segCount; i++){
            Segment seg = new Segment(i, storeAddress + segSize * i, this);
            segmentsQueue.add(seg);
            segments[i] = seg;
        }
    }
    public boolean dispose () throws IOException {
        getUnsafe().freeMemory(storeAddress);
        return true;
    }
    public static sun.misc.Unsafe getUnsafe() {
        return UnsafeUtils.unsafe;
    }
    public void removeCellFromIndex(long hash){
        synchronized (cellIndex) {
            cellIndex.remove(hash);
        }
    }
    public boolean hasCell (long hash){
        return getCellIndex().containsKey(hash);
    }
    public long getCellAddr(long hash){
        return getCellIndex().get(hash);
    }
    public void putTombstone (long startPos, long endPos){
        int size = (int) (endPos - startPos + 1);
        assert  size > tombstoneSize : "frag length is too small to put a tombstone";
        Writer.writeByte((byte) 2, startPos);
        Writer.writeInt(size, startPos + byteLen);

    }
    public void addFragment (long startPos, long endPos) {
        cleaner.addFragment(startPos, endPos);
    }
    public void addDirtyRanges (long startPos, long endPos) {
        if (backendEnabled) {
            Segment segment = locateSegment(startPos);
            segment.setDirty();
        }
    }

    public void enableDurability () {
        this.backendEnabled = true;
    }


    public void copyMemoryForCleaner(long startPos, long target, long len){
        assert startPos >= this.storeAddress && startPos < this.storeAddress + this.getSize();
        assert target >= this.storeAddress && target < this.storeAddress + this.getSize();
        assert target + len <= this.storeAddress + this.getSize();
        getUnsafe().copyMemory(startPos, target, len);
//        long dirtyEndPos = target + len - 1;
//        addDirtyRanges(target, dirtyEndPos);
    }

    private boolean hasSpaces(long size) {
        for (Segment segment : this.segments) {
            if (Trunk.getSegSize() - segment.getAliveObjectBytes() > size) {
                this.cleaner.phaseOneCleanSegment(segment);
                if (Trunk.getSegSize() - (segment.getCurrentLoc() - segment.getBaseAddr()) > size) {
                    return true;
                }
            }
        }
        return false;
    }

    public long tryAcquireSpace (long length) throws ObjectTooLargeException {
        if (length > maxObjSize) {
            throw new ObjectTooLargeException(length + " of " + maxObjSize);
        }
        long r = -1;
        int turn = 0;
        long acquireTimeSpan = System.currentTimeMillis();
        Segment firstSeg = segmentsQueue.peek();
        for (Segment seg : segmentsQueue) {
            try {
                seg.lockRead();
                r = seg.tryAcquireSpace(length);
                if (r > 0) {
                    break;
                } else {
                    segmentsQueue.remove(seg);
                    segmentsQueue.offer(seg);
                    if (turn > 0 && seg == firstSeg && (!hasSpaces(length) || System.currentTimeMillis() - acquireTimeSpan > 60000)) {
                        break;
                    }
                }
            } finally {
                seg.unlockRead();
            }
            turn ++;
        }
        return r;
    }

    public Segment locateSegment (long starts) {
        long relativeLoc = starts - storeAddress;
        int segId = (int) Math.floor(relativeLoc / Trunk.segSize);
        Segment seg = null;
        try {
            seg = segments[segId];
        } catch (ArrayIndexOutOfBoundsException ex) {
            System.out.println("Cannot locate seg: " + starts + " " + storeAddress + " " + relativeLoc + " " + segId);
            throw ex;
        }
        return seg;
    }

    public List<Segment> getDirtySegments () {
        return Arrays.stream(segments).filter(Segment::isDirty).collect(Collectors.toList());
    }

    public long getStoreAddress() {
        return storeAddress;
    }
}
