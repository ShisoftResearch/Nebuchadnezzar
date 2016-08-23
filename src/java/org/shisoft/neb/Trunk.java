package org.shisoft.neb;

import net.openhft.koloboke.collect.map.LongLongMap;
import net.openhft.koloboke.collect.map.LongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongLongMaps;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import org.shisoft.neb.exceptions.ObjectTooLargeException;
import org.shisoft.neb.io.Writer;
import org.shisoft.neb.utils.UnsafeUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
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
    final static int cellLockCount = 2048;

    private int id;
    private long storeAddress;
    private long size;
    private LongLongMap cellIndex = HashLongLongMaps.newMutableMap();
    private LongObjMap<CellLock> cellLockCache = HashLongObjMaps.newMutableMap();
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
    public LongLongMap  getCellIndex() {
        return cellIndex;
    }
    public int getId() {
        return id;
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

    public Trunk(long size, int id){
        this(size, id, 8);
    }

    public Trunk(long size, int id, int trunkCount){
        this.size = size;
        this.id = id;
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
    }

    private boolean hasSpaces(long size, Segment lockedSegment) {
        for (Segment segment : this.segments) {
            if (Trunk.getSegSize() - segment.getAliveObjectBytes() > size) {
                this.cleaner.phaseOneCleanSegment(segment);
                boolean hashSpace = Trunk.getSegSize() - (segment.getCurrentLoc() - segment.getBaseAddr()) > size;
                return true;
            }
        }
        return false;
    }

    public void putToBack(Segment seg) {
        segmentsQueue.remove(seg);
        segmentsQueue.offer(seg);
    }

    public long tryAcquireSpace (long length) throws ObjectTooLargeException {
        if (length > maxObjSize) {
            throw new ObjectTooLargeException(length + " of " + maxObjSize);
        }
        long r = -1;
        int turn = 0;
        Segment firstSeg = segmentsQueue.peek();
        while (true) {
            try {
                Segment seg = segmentsQueue.peek();
                if (!seg.getLock().readLock().tryLock()) {
                    putToBack(seg);
                    continue;
                }
                try {
                    r = seg.tryAcquireSpace(length);
                } finally {
                    seg.unlockRead();
                }
                if (r > 0) {
                    break;
                } else {
                    putToBack(seg);
                    if (turn > 1 && firstSeg == seg) {
                        if (!hasSpaces(length, seg)) {
                            break;
                        }
                    }
                }
            } finally {
                turn ++;
            }
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

    public long getStoreAddress () {
        return storeAddress;
    }

    public CellLock getCellLock (long hash) {
        synchronized (cellLockCache) {
            CellLock cl = cellLockCache.get(hash);
            if (cl == null) {
                cl = new CellLock(hash);
                cellLockCache.put(hash, cl);
            }
            cl.getAcquired().getAndIncrement();
            return cl;
        }
    }

    public void releaseCellLock (CellLock cl) {
        cl.getAcquired().updateAndGet(operand -> {
            --operand;
            if (operand == 0) {
                synchronized (cellLockCache) {
                    cellLockCache.remove(cl.getHash(), cl);
                    return -1;
                }
            } else if (operand < 0) {
                System.out.println("WARN: acquired lock below 0");
            }
            return operand;
        });
    }

    public CellLock lockWrite (long hash) {
        CellLock cl = getCellLock(hash);
        cl.getLock().writeLock().lock();
        return cl;
    }


    public void unlockWrite (CellLock cl) {
        cl.getLock().writeLock().unlock();
        releaseCellLock(cl);
    }

}
