package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashIntObjMap;
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;
import org.shisoft.neb.io.cellMeta;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by shisoft on 18/1/2016.
 */
public class trunk {
    volatile byte[] store;
    HashIntObjMap<cellMeta> cellIndex = HashIntObjMaps.newMutableMap();
    AtomicInteger pointer = new AtomicInteger(0);
    ConcurrentSkipListMap<Integer, Integer> fragments = new ConcurrentSkipListMap<Integer, Integer>();
    public trunk(int size){
        store = new byte[size];
    }
    public byte[] getStore() {
        return store;
    }
    public AtomicInteger getPointer() {
        return pointer;
    }
    public ConcurrentSkipListMap getFragments() {
        return fragments;
    }
    public HashIntObjMap<cellMeta> getCellIndex() {
        return cellIndex;
    }
    public int getCellLoc(int hash){
        return cellIndex.get(hash).getLocation();
    }
    public boolean dispose (){
        this.store = null;
        return true;
    }
    public void removeCellFromIndex(int hash){
        getCellIndex().remove(hash);
    }

    public boolean hasCell (int hash){
        return getCellIndex().containsKey(hash);
    }

    public void addFragment (int startPos, int endPos) throws Exception {
        if (fragments.containsKey(startPos)){
            throw new Exception("fragment at pos " + startPos + " already exists");
        }
        fragments.put(startPos, endPos);
    }
}
