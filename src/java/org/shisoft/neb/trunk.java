package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by shisoft on 18/1/2016.
 */
public class trunk {
    byte[] store;
    HashIntIntMap cellIndex = HashIntIntMaps.newMutableMap();
    AtomicInteger pointer = new AtomicInteger(0);
    public trunk(int size){
        store = new byte[size];
    }
    public byte[] getStore() {
        return store;
    }
    public AtomicInteger getPointer() {
        return pointer;
    }
    public HashIntIntMap getCellIndex() {
        return cellIndex;
    }
    public boolean dispose (){
        this.store = null;
        return true;
    }
}
