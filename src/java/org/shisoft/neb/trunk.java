package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import org.shisoft.neb.io.cellMeta;
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
    private static final Unsafe unsafe;
    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private long storeAddress;
    long size;
    HashLongObjMap<cellMeta> cellIndex = HashLongObjMaps.newMutableMap();
    AtomicLong appendHeader = new AtomicLong(0);
    ConcurrentSkipListMap<Long, Long> fragments = new ConcurrentSkipListMap<Long, Long>();
    ReentrantLock fragsLock = new ReentrantLock();
    ReentrantLock cellWriterLock = new ReentrantLock();
    public trunk(long size){
        this.size = size;
        storeAddress = unsafe.allocateMemory(size);
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
    public HashLongObjMap<cellMeta> getCellIndex() {
        return cellIndex;
    }
    public long getCellLoc(long hash){
        return cellIndex.get(hash).getLocation();
    }
    public boolean dispose (){
        unsafe.freeMemory(storeAddress);
        return true;
    }
    public static Unsafe getUnsafe() {
        return unsafe;
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

    public void addFragment (long startPos, long endPos) throws Exception {
        lockFrags();
        try {
            addFragment_(startPos, endPos);
        } finally {
            unlockFrags();
        }
    }

    public void addFragment_ (long startPos, long endPos) throws Exception {
        Long seqFPos = endPos + 1;
        Long seqBPos = startPos - 1;
        Long seqFrag = fragments.get(seqFPos);
        if (seqFrag != null){
            addFragment_(startPos, seqFrag);
            removeFrag(seqFPos);
        } else {
            Map.Entry<Long, Long> fe = fragments.floorEntry(seqBPos);
            if (fe != null && fe.getValue().equals(seqBPos)){
                addFragment_(fe.getKey(), endPos);
            } else {
                fragments.put(startPos, endPos);
            }
        }
    }

    public int countFragments (){
        return fragments.size();
    }

    public void removeFrag (long startPos){
        fragments.remove(startPos);
    }

    public void lockFrags(){
        fragsLock.lock();
    }
    public void unlockFrags(){
        fragsLock.unlock();
    }
    public void resetAppendHeader(Long loc){
        appendHeader.set(loc);
    }
    public long getAppendHeaderValue (){
        return appendHeader.get();
    }
    public void copyMemory(Long startPos, Long target, Long len){
        getUnsafe().copyMemory(storeAddress + startPos, storeAddress + target, len);
    }
}
