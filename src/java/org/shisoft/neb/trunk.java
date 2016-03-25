package org.shisoft.neb;

import clojure.lang.Obj;
import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import org.shisoft.neb.io.cellMeta;
import org.shisoft.neb.utils.unsafe;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by shisoft on 18/1/2016.
 */
public class trunk {
    private long storeAddress;
    long size;
    HashLongObjMap<cellMeta> cellIndex = HashLongObjMaps.newMutableMap();
    AtomicLong appendHeader = new AtomicLong(0);
    ConcurrentSkipListMap<Long, Long> fragments = new ConcurrentSkipListMap<>();
    ConcurrentSkipListMap<Long, Long> dirtyRanges = new ConcurrentSkipListMap<>();
    ReentrantLock cellWriterLock = new ReentrantLock();
    boolean hasBackend = false;
    public trunk(long size){
        this.size = size;
        storeAddress = getUnsafe().allocateMemory(size);
    }

    public boolean isHasBackend() {
        return hasBackend;
    }

    public void setHasBackend(boolean hasBackend) {
        this.hasBackend = hasBackend;
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
    public synchronized HashLongObjMap<cellMeta> getCellIndex() {
        return cellIndex;
    }
    public long getCellLoc(long hash){
        return cellIndex.get(hash).getLocation();
    }
    public boolean dispose (){
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
    public void addFragment (long startPos, long endPos) {
        addAndAutoMerge(fragments, startPos, endPos);
    }
    public void addDirtyRanges (long startPos, long endPos) {
        if (hasBackend) {
            addAndAutoMerge(dirtyRanges, startPos, endPos);
        }
    }
    public void addAndAutoMerge (ConcurrentSkipListMap<Long, Long> map, long startPos, long endPos) {
        Long seqFPos = endPos + 1;
        Long seqBPos = startPos - 1;
        Long seqFrag = map.get(seqFPos);
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
