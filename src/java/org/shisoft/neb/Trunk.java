package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import org.shisoft.neb.exceptions.ObjectTooLargeException;
import org.shisoft.neb.exceptions.StoreFullException;
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
    final static int maxObjSize = 2 * 1024 * 1024;
    final static int segSize    = 8 * 1024 * 1024;

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
    private void initSegments (int size) {
        int segCount = (int) Math.floor((double) this.size / size);
        segmentsQueue = new ConcurrentLinkedQueue<>();
        segments = new Segment[segCount];
        for (int i = 0; i < segCount; i++){
            Segment seg = new Segment(storeAddress + size * i, this);
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
        Writer.writeByte((byte) 1, startPos);
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
        this.dirtyRanges = new ConcurrentSkipListMap<Long, Long>();
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

    public long tryAcquireSpace (int length) throws ObjectTooLargeException {
        if (length > maxObjSize) {
            throw new ObjectTooLargeException(length + " of " + maxObjSize);
        }
        long r = -1;
        for (Segment seg : segmentsQueue) {
            r = seg.tryAcquireSpace(length);
            if (r > 0) {
                break;
            } else {
                segmentsQueue.remove(seg);
                segmentsQueue.offer(seg);
            }
        }
        return r;
    }

    public Segment locateSegment (long starts) {
        long relativeLoc = starts - storeAddress;
        int segId = (int) Math.floor(relativeLoc / Trunk.segSize);
        return segments[segId];
    }

    public long getStoreAddress() {
        return storeAddress;
    }

    public void readLockSegment (CellMeta meta) {
        locateSegment(meta.getLocation()).getLock().readLock().lock();
    }

    public void readUnlockSegment (CellMeta meta) {
        locateSegment(meta.getLocation()).getLock().readLock().unlock();
    }
}
