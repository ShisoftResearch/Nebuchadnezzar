package org.shisoft.neb;

import clojure.lang.IFn;
import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import org.shisoft.neb.io.CellMeta;
import org.shisoft.neb.io.Writer;
import org.shisoft.neb.utils.Collection;
import org.shisoft.neb.utils.UnsafeUtils;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.shisoft.neb.io.type_lengths.*;

/**
 * Created by shisoft on 18/1/2016.
 */
public class Trunk {

    public final static int tombstoneSize = intLen + 1;

    private int id;
    private long storeAddress;
    private long size;
    private HashLongObjMap<CellMeta> cellIndex = HashLongObjMaps.newMutableMap();
    private AtomicLong appendHeader = new AtomicLong(0);
    private ConcurrentSkipListMap<Long, Long> dirtyRanges;
    private MemoryFork memoryFork;
    private boolean backendEnabled = false;
    private ReentrantLock cellWriteLock = new ReentrantLock();
    private Defragmentation defrag;
    private volatile boolean slowMode = false;
    public boolean isBackendEnabled() {
        return backendEnabled;
    }
    public long getSize() {
        return size;
    }
    public AtomicLong getAppendHeader() {
        return appendHeader;
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
    public ReentrantLock getCellWriteLock() {
        return cellWriteLock;
    }
    public Defragmentation getDefrag() {
        return defrag;
    }
    public Trunk(long size){
        this.size = size;
        storeAddress = getUnsafe().allocateMemory(size);
        defrag = new Defragmentation(this);
    }
    public boolean dispose () throws IOException {
        getUnsafe().freeMemory(storeAddress);
        return true;
    }
    public static sun.misc.Unsafe getUnsafe() {
        return UnsafeUtils.unsafe;
    }
    public long getStoreAddress() {
        return storeAddress;
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
        long size = endPos - startPos + 1;
        long tombstoneEnds = startPos + byteLen + longLen - 1;
        assert  size > tombstoneSize : "frag length is too small to put a tombstone";
        copyMemForFork(startPos, tombstoneEnds);
        Writer.writeByte(this, (byte) 1, startPos);
        Writer.writeLong(this, size, startPos + byteLen);
        addDirtyRanges(startPos, tombstoneEnds);
    }
    public void addFragment (long startPos, long endPos) {
        defrag.addFragment(startPos, endPos);
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

    public int countFragments (){
        return defrag.countFragments();
    }

    public ConcurrentSkipListMap<Long, Long> getDirtyRanges() {
        return dirtyRanges;
    }

    public void removeFrag (long startPos, long endPos){
        defrag.removeFrag(startPos, endPos);
    }
    public long getAppendHeaderValue (){
        return appendHeader.get();
    }
    public void copyMemory(long startPos, long target, long len){
        long dirtyEndPos = target + len - 1;
        copyMemForFork(target, dirtyEndPos);
        getUnsafe().copyMemory(storeAddress + startPos, storeAddress + target, len);
        addDirtyRanges(target, dirtyEndPos);
    }
    public MemoryFork fork(){
        assert memoryFork == null : "Only one folk allowed";
        memoryFork = new MemoryFork(this);
        return memoryFork;
    }

    public long tryAcquireFromAppendHeader (long length, AtomicBoolean overflowed, CellMeta meta){
        try {
            cellWriteLock.lock();
            Long cellLoc = meta != null ? meta.getLocation() : null;
            long r = getAppendHeader().getAndUpdate(appenderLoc -> {
                long expectedLoc = appenderLoc + length;
                if (expectedLoc > size) {
                    overflowed.set(true);
                    if (meta != null) {meta.setLocation(cellLoc);}
                    return appenderLoc;
                } else {
                    overflowed.set(false);
                    if (meta != null && meta.getLocation() < 0) {meta.setLocation(appenderLoc);}
                    return expectedLoc;
                }
            });
            return r;
        } finally {
            cellWriteLock.unlock();
        }
    }

    public void enterSlowMode () {
        this.slowMode = true;
    }
    public void leaveSlowMode () {
        this.slowMode = false;
    }
    public boolean isSlowMode() {
        return slowMode;
    }
    public boolean isCurrentOpSlowMode() {
        return slowMode && cellWriteLock.isHeldByCurrentThread();
    }
    public void tryWriteCell () {
        if (slowMode && !this.cellWriteLock.isHeldByCurrentThread()) this.cellWriteLock.lock();
    }
    public void endWriteCell () {
        if (this.cellWriteLock.isHeldByCurrentThread()) this.cellWriteLock.unlock();
    }
    public float computeFillRatio () {
        long trunkSize = getSize();
        long appendHeader = getAppendHeaderValue();
        float r = (float) appendHeader / (float) trunkSize;
        return r;
    }
    public boolean checkShouldInSlowMode (float fillRate) {
        boolean almostFull = fillRate > 0.9;
        boolean originalMode = isSlowMode();
        if (almostFull) {
            enterSlowMode();
        } else {
            leaveSlowMode();
        }
        if (isSlowMode() != originalMode) {
            System.out.println(isSlowMode() ? "In Slow Mode" : "Leave Slow Mode");
        }
        return almostFull;
    }
    public boolean checkShouldInSlowMode () {
        return checkShouldInSlowMode(computeFillRatio());
    }

    public CellMeta addCellMetaToTrunkIndex(long hash, CellMeta meta) throws Exception {
        synchronized (cellIndex) {
            if (cellIndex.putIfAbsent(hash, meta) != null) {
                throw new Exception("Cell hash already exists");
            }
        }
        return meta;
    }
}
