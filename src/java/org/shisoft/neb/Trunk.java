package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import org.shisoft.neb.io.CellMeta;
import org.shisoft.neb.io.Writer;
import org.shisoft.neb.io.type_lengths;
import org.shisoft.neb.utils.UnsafeUtils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by shisoft on 18/1/2016.
 */
public class Trunk {
    private int id;
    private long storeAddress;
    private long size;
    private HashLongObjMap<CellMeta> cellIndex = HashLongObjMaps.newMutableMap();
    private AtomicLong appendHeader = new AtomicLong(0);
    private final ConcurrentSkipListMap<Long, Long> fragments = new ConcurrentSkipListMap<>();
    private ConcurrentSkipListMap<Long, Long> dirtyRanges;
    private MemoryFork memoryFork;
    private boolean backendEnabled = false;
    public boolean isBackendEnabled() {
        return backendEnabled;
    }
    public long getSize() {
        return size;
    }
    public AtomicLong getAppendHeader() {
        return appendHeader;
    }
    public ConcurrentSkipListMap getFragments() {
        return fragments;
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
    public Trunk(long size){
        this.size = size;
        storeAddress = getUnsafe().allocateMemory(size);
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
        getCellIndex().remove(hash);
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
        long tombstoneEnds = startPos + type_lengths.byteLen + type_lengths.longLen - 1;
        assert  size > (type_lengths.intLen + 1) : "frag length is too small to put a tombstone";
        copyMemForFork(startPos, tombstoneEnds);
        Writer.writeByte(this, (byte) 1, startPos);
        Writer.writeLong(this, size, startPos + type_lengths.byteLen);
        addDirtyRanges(startPos, tombstoneEnds);
    }
    public void addFragment (long startPos, long endPos) {
        synchronized (fragments) {
            Map.Entry<Long, Long> actualRange = addAndAutoMerge(fragments, startPos, endPos);
            putTombstone(actualRange.getKey(), actualRange.getValue());
        }
    }
    public void addDirtyRanges (long startPos, long endPos) {
        if (backendEnabled) {
            synchronized (dirtyRanges) {
                addAndAutoMerge(dirtyRanges, startPos, endPos);
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
    public Map.Entry<Long, Long> addAndAutoMerge (ConcurrentSkipListMap<Long, Long> map, long startPos, long endPos) {
        Map.Entry<Long, Long> prevPair = map.lowerEntry(startPos);
        Map.Entry<Long, Long> forPair = map.higherEntry(startPos);
        Long dupLoc = map.get(startPos);
        if (dupLoc != null && dupLoc >= endPos) return null;
        if (prevPair != null && prevPair.getValue() >= endPos) return null;
        if (prevPair != null && (prevPair.getValue() >= startPos || prevPair.getValue() == startPos - 1)) {
            return addAndAutoMerge(map, prevPair.getKey(), endPos);
        } else if (forPair != null && (forPair.getKey() < endPos || forPair.getKey() == endPos + 1)) {
            map.remove(forPair.getKey());
            if (forPair.getValue() < endPos){
                return addAndAutoMerge(map, startPos, endPos);
            } else {
                return addAndAutoMerge(map, startPos, forPair.getValue());
            }
        } else {
            map.put(startPos, endPos);
            return new Map.Entry<Long, Long>() {
                public Long getKey() {
                    return startPos;
                }
                public Long getValue() {
                    return endPos;
                }
                public Long setValue(Long value) {
                    return null;
                }
            };
        }
    }

    public int countFragments (){
        return fragments.size();
    }

    public ConcurrentSkipListMap<Long, Long> getDirtyRanges() {
        return dirtyRanges;
    }

    public void removeFrag (long startPos){
        fragments.remove(startPos);
    }
    public void resetAppendHeader(Long loc){
        appendHeader.set(loc);
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
}
