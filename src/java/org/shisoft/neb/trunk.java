package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
import net.openhft.koloboke.collect.map.hash.HashIntObjMap;
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;
import org.shisoft.neb.io.cellLock;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

/**
 * Created by shisoft on 18/1/2016.
 */
public class trunk {
    byte[] store;
    HashIntIntMap cellIndex = HashIntIntMaps.newMutableMap();
    HashIntObjMap<cellLock> locks = HashIntObjMaps.newMutableMap();
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
    public HashIntIntMap getCellIndex() {
        return cellIndex;
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

    private cellLock obtainLock (int hash){
        return locks.computeIfAbsent(hash, new IntFunction<cellLock>() {
            @Override
            public cellLock apply(int value) {
                cellLock lock = new cellLock();
                lock.init();
                return lock;
            }
        });
    }

    public void lockWrite(int hash){
        obtainLock(hash).lockWrite();
    }

    public void unlockWrite(int hash){
        cellLock lock = obtainLock(hash);
        lock.unlockWrite();
        checkLock(hash, lock);
    }

    public void lockRead(int hash){
        obtainLock(hash).lockRead();
    }

    public void unlockRead(int hash){
        cellLock lock = obtainLock(hash);
        lock.unlockRead();
        checkLock(hash, lock);
    }

    public void checkLock(int hash, cellLock lock){
        if (lock.getOperationsInProgress() == 0){
            locks.remove(hash);
        }
    }
}
