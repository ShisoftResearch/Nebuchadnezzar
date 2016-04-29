package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import org.shisoft.neb.exceptions.ObjectTooLargeException;
import org.shisoft.neb.io.CellMeta;
import org.shisoft.neb.io.Writer;
import org.shisoft.neb.utils.UnsafeUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.shisoft.neb.io.type_lengths.*;

/**
 * Created by shisoft on 18/1/2016.
 */
public class Trunk {

    final static int tombstoneSize = intLen + 1;
    final static int segSize    = 8 * 1024 * 1024; //8M Segment
    final static int maxObjSize = 1 * 1024 * 1024; //1M Object

    private int id;
    private long storeAddress;
    private long size;
    private HashLongObjMap<CellMeta> cellIndex = HashLongObjMaps.newMutableMap();
    private boolean backendEnabled = false;
    private Cleaner cleaner;
    private ConcurrentLinkedQueue<Segment> segmentsQueue;
    private Segment[] segments;
    public boolean isBackendEnabled() {
        return backendEnabled;
    }
    public ConcurrentLinkedQueue<Segment> getSegmentsQueue() {
        return segmentsQueue;
    }
    public long getSize() {
        return size;
    }
    public HashLongObjMap<CellMeta> getCellIndex() {
        return cellIndex;
    }
    public long getCellLoc(long hash){
        return cellIndex.get(hash).getLocation();
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
        initSegments(segSize);
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
    public Object getCellMeta(long hash){
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

    public boolean hasSpaces(long size) {
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
                seg.lockWrite();
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
                seg.unlockWrite();
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
        return Arrays.asList(segments).stream().filter(Segment::isDirty).collect(Collectors.toList());
    }

    public long getStoreAddress() {
        return storeAddress;
    }

    public void readMetaLockSegment(CellMeta meta) {
        locateSegment(meta.getLocation()).getLock().readLock().lock();
    }

    public void readUnlockMetaSegment(CellMeta meta) {
        locateSegment(meta.getLocation()).getLock().readLock().unlock();
    }
}
