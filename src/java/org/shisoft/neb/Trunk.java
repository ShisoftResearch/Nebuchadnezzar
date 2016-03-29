package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import org.shisoft.neb.io.CellMeta;
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
    private ConcurrentSkipListMap<Long, Long> fragments = new ConcurrentSkipListMap<>();
    private ConcurrentSkipListMap<Long, Long> dirtyRanges;
    private ReentrantLock cellWriterLock = new ReentrantLock();
    private MemoryFork memoryFork;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean backendEnabled = false;
    public boolean isBackendEnabled() {
        return backendEnabled;
    }
    public long getSize() {
        return size;
    }
    public ReentrantLock getCellWriterLock() {
        return cellWriterLock;
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
    public synchronized void addFragment (long startPos, long endPos) {
        addAndAutoMerge(fragments, startPos, endPos);
    }
    public synchronized void addDirtyRanges (long startPos, long endPos) {
        if (backendEnabled) {
            addAndAutoMerge(dirtyRanges, startPos, endPos);
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
    public void addAndAutoMerge (ConcurrentSkipListMap<Long, Long> map, long startPos, long endPos) {
        Map.Entry<Long, Long> prevPair = map.lowerEntry(startPos);
        Map.Entry<Long, Long> forPair = map.higherEntry(startPos);
        Long dupLoc = map.get(startPos);
        if (dupLoc != null && dupLoc >= endPos) return;
        if (prevPair != null && prevPair.getValue() >= endPos) return;
        if (prevPair != null && (prevPair.getValue() >= startPos || prevPair.getValue() == startPos - 1)) {
            addAndAutoMerge(map, prevPair.getKey(), endPos);
        } else if (forPair != null && (forPair.getKey() < endPos || forPair.getKey() == endPos + 1)) {
            map.remove(forPair.getKey());
            if (forPair.getValue() < endPos){
                addAndAutoMerge(map, startPos, endPos);
            } else {
                addAndAutoMerge(map, startPos, forPair.getValue());
            }
        } else {
            map.put(startPos, endPos);
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
    public void copyMemory(Long startPos, Long target, Long len){
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
    public void readLock(){
        this.lock.readLock().lock();
    }
    public void readUnLock(){
        this.lock.readLock().unlock();
    }
    public void writeLock(){
        this.lock.writeLock().lock();
    }
    public void writeUnlock(){
        if (this.lock.writeLock().isHeldByCurrentThread()) {
            this.lock.writeLock().unlock();
        }
    }
}
