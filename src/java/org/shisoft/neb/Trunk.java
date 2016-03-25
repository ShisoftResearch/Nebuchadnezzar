package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import org.shisoft.neb.durability.BackStore;
import org.shisoft.neb.io.CellMeta;
import org.shisoft.neb.utils.unsafe;
import sun.misc.Unsafe;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by shisoft on 18/1/2016.
 */
public class Trunk {
    private long storeAddress;
    long size;
    HashLongObjMap<CellMeta> cellIndex = HashLongObjMaps.newMutableMap();
    AtomicLong appendHeader = new AtomicLong(0);
    ConcurrentSkipListMap<Long, Long> fragments = new ConcurrentSkipListMap<>();
    ConcurrentSkipListMap<Long, Long> dirtyRanges = new ConcurrentSkipListMap<>();
    ReentrantLock cellWriterLock = new ReentrantLock();
    BackStore backStore;
    boolean hasBackend = false;
    public boolean isHasBackend() {
        return hasBackend;
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
    public synchronized HashLongObjMap<CellMeta> getCellIndex() {
        return cellIndex;
    }
    public long getCellLoc(long hash){
        return cellIndex.get(hash).getLocation();
    }
    public Trunk(long size){
        this.size = size;
        storeAddress = getUnsafe().allocateMemory(size);
    }
    public boolean dispose () throws IOException {
        getUnsafe().freeMemory(storeAddress);
        return true;
    }
    public static Unsafe getUnsafe() {
        return unsafe.unsafe;
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
    public synchronized void addFragment (long startPos, long endPos) {
        addAndAutoMerge(fragments, startPos, endPos);
    }
    public synchronized void addDirtyRanges (long startPos, long endPos) {
        if (hasBackend) {
            addAndAutoMerge(dirtyRanges, startPos, endPos);
        }
    }
    public void setBackStore (String basePath) throws IOException {
        this.backStore = new BackStore(basePath);
    }
    public void addAndAutoMerge (ConcurrentSkipListMap<Long, Long> map, long startPos, long endPos) {
        Long seqFPos = endPos + 1;
        Long seqBPos = startPos - 1;
        Long seqFrag = map.get(seqFPos);
        Long dupLoc = map.get(startPos);
        if (dupLoc != null && dupLoc >= endPos) return;
        if (seqFrag != null){
            map.remove(seqFPos);
            addAndAutoMerge(map, startPos, seqFrag);
        } else {
            Map.Entry<Long, Long> fe = map.floorEntry(seqBPos);
            if (fe != null && fe.getValue().equals(seqBPos)){
                addAndAutoMerge(map, fe.getKey(), endPos);
            } else {
                map.put(startPos, endPos);
            }
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
        getUnsafe().copyMemory(storeAddress + startPos, storeAddress + target, len);
        addDirtyRanges(target, target + len - 1);
    }
}
