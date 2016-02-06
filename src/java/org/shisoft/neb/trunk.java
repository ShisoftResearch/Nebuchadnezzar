package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import org.shisoft.neb.io.cellMeta;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

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
    HashLongObjMap<cellMeta> cellIndex = HashLongObjMaps.newMutableMap();
    AtomicLong pointer = new AtomicLong(0);
    ConcurrentSkipListMap<Long, Long> fragments = new ConcurrentSkipListMap<Long, Long>();
    public trunk(long size){
        storeAddress = unsafe.allocateMemory(size);
    }
    public byte getMemory(long addr) {
        return unsafe.getByte(storeAddress + addr);
    }
    public void setMemory(long addr, byte b) {
        unsafe.putByte(storeAddress + addr, b);
    }
    public AtomicLong getPointer() {
        return pointer;
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
        if (fragments.containsKey(startPos)){
            throw new Exception("fragment at pos " + startPos + " already exists");
        }
        fragments.put(startPos, endPos);
    }
}
