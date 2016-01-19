package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by shisoft on 18/1/2016.
 */
public class trunk {

    private static Unsafe unsafe;
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
}
