package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import org.shisoft.neb.exceptions.ObjectTooLargeException;
import org.shisoft.neb.io.CellMeta;
import org.shisoft.neb.io.Writer;
import org.shisoft.neb.utils.Collection;
import org.shisoft.neb.utils.UnsafeUtils;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.shisoft.neb.io.type_lengths.*;

/**
 * Created by shisoft on 18/1/2016.
 */
public class Trunk {

    final static int tombstoneSize = intLen + 1;
    final static int maxObjSize = 8 * 1024 * 1024;
    final static int segSize    = 64 * 1024 * 1024;

    private int id;
    private long storeAddress;
    private long size;
    private HashLongObjMap<CellMeta> cellIndex = HashLongObjMaps.newMutableMap();
    private ConcurrentSkipListMap<Long, Long> dirtyRanges;
    private MemoryFork memoryFork;
    private boolean backendEnabled = false;
    private Cleaner cleaner;
    private volatile boolean slowMode = false;
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
    public void setMemoryFork(MemoryFork memoryFork) {
        this.memoryFork = memoryFork;
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
            Segment seg = new Segment(storeAddress + segSize * i, this);
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
    public MemoryFork getMemoryFork() {
        return memoryFork;
    }
    public void putTombstone (long startPos, long endPos){
        int size = (int) (endPos - startPos + 1);
        long tombstoneEnds = startPos + byteLen + longLen - 1;
        assert  size > tombstoneSize : "frag length is too small to put a tombstone";
        copyMemForFork(startPos, tombstoneEnds);
        Writer.writeByte((byte) 2, startPos);
        Writer.writeInt(size, startPos + byteLen);
        addDirtyRanges(startPos, tombstoneEnds);
    }
    public void addFragment (long startPos, long endPos) {
        cleaner.addFragment(startPos, endPos);
    }
    public void addDirtyRanges (long startPos, long endPos) {
        if (backendEnabled) {
            synchronized (dirtyRanges) {
                Collection.addAndAutoMerge(dirtyRanges, startPos, endPos);
            }
        }
    }
    public void copyMemForFork(long start, long end){
        if (memoryFork != null){
            memoryFork.copyMemory(start, end);
        }
    }
    public void enableDurability () {
        this.dirtyRanges = new ConcurrentSkipListMap<>();
        this.backendEnabled = true;
    }

    public ConcurrentSkipListMap<Long, Long> getDirtyRanges() {
        return dirtyRanges;
    }

    public void copyMemory(long startPos, long target, long len){
        long dirtyEndPos = target + len - 1;
        copyMemForFork(target, dirtyEndPos);
        getUnsafe().copyMemory(startPos, target, len);
        addDirtyRanges(target, dirtyEndPos);
    }
    public MemoryFork fork(){
        assert memoryFork == null : "Only one folk allowed";
        memoryFork = new MemoryFork(this);
        return memoryFork;
    }

    public long tryAcquireSpace (long length) throws ObjectTooLargeException {
        if (length > maxObjSize) {
            throw new ObjectTooLargeException(length + " of " + maxObjSize);
        }
        long r = -1;
        int turn = 0;
        Segment firstSeg = segmentsQueue.peek();
        for (Segment seg : segmentsQueue) {
            r = seg.tryAcquireSpace(length);
            if (r > 0) {
                break;
            } else {
                segmentsQueue.remove(seg);
                segmentsQueue.offer(seg);
                if (turn > 0 && seg == firstSeg) {
                    break;
                }
            }
            turn ++;
        }
        return r;
    }

    public Segment locateSegment (long starts) {
        long relativeLoc = starts - storeAddress;
        assert relativeLoc >= 0;
        int segId = (int) Math.floor(relativeLoc / Trunk.segSize);
        assert segId >= 0;
        Segment seg = null;
        try {
            seg = segments[segId];
        } catch (ArrayIndexOutOfBoundsException ex) {
            System.out.println("Cannot locate seg: " + starts + " " + storeAddress + " " + relativeLoc + " " + segId);
            throw ex;
        }
        return seg;
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
