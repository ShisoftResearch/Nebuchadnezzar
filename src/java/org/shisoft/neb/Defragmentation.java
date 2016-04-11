package org.shisoft.neb;

import org.shisoft.neb.io.CellMeta;
import org.shisoft.neb.utils.Bindings;
import org.shisoft.neb.utils.Collection;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by shisoft on 16-4-11.
 */
public class Defragmentation {
    private Trunk trunk;
    private volatile long currentLoc = -1;
    private volatile long currentSize = 0;
    private volatile long lastDefraged = 0;
    private ReentrantLock defragLock = new ReentrantLock();
    private ReentrantLock opLock = new ReentrantLock();
    private long cellHeadLen = (long) Bindings.cellHeadLen.invoke();
    private final ConcurrentSkipListMap<Long, Long> fragments = new ConcurrentSkipListMap<>();
    public Defragmentation(Trunk trunk) {
        this.trunk = trunk;
    }
    public void removeFrag (long startPos, long endPos){
        fragments.remove(startPos, endPos);
    }
    public int countFragments (){
        return fragments.size();
    }
    public void lockDefrag(){
        this.defragLock.lock();
    }
    public void unlockDefrag(){
        this.defragLock.unlock();
    }
    public void addFragment (long startPos, long endPos) {
        synchronized (fragments) {
            Map.Entry<Long, Long> actualRange = Collection.addAndAutoMerge(fragments, startPos, endPos);
            if (actualRange != null) {
                trunk.putTombstone(actualRange.getKey(), actualRange.getValue());
            }
        }
    }
    public void defrag (){
        Map.Entry<Long, Long> frag;
        long appendHeader;
        long lwPos, hiPos, hnPos;
        lockDefrag();
        try {
            while (true) {
                try {
                    opLock.lock();
                    frag = fragments.firstEntry();
                    appendHeader = trunk.getAppendHeaderValue();
                    if (frag == null) break;
                    lwPos = frag.getKey();hiPos = frag.getValue();hnPos = hiPos + 1;
                    if (!fragments.remove(lwPos, hiPos)) continue;
                    if (appendHeader >= lwPos && appendHeader <= hiPos + 1) {
                        boolean rested = trunk.getAppendHeader().compareAndSet(appendHeader, frag.getKey());
                        if (rested) break;
                    } else {
                        long cellHash = (long) Bindings.readCellHash.invoke(trunk, hnPos);
                        long cellLen = (int) Bindings.readCellLength.invoke(trunk, hnPos);
                        cellLen += cellHeadLen;
                        CellMeta meta = trunk.getCellIndex().get(cellHash);
                        if (meta != null) {
                            boolean copied = false;
                            synchronized (meta) {
                                if (meta.getLocation() == hnPos) {
                                    trunk.copyMemory(hnPos, lwPos, cellLen);
                                    meta.setLocation(lwPos);
                                    copied = true;
                                }
                            }
                            if (copied) {
                                addFragment(lwPos + cellLen, hnPos + cellLen -1);
                            }
                        }
                    }

                } finally {
                    opLock.unlock();
                }
            }
        } finally {
            unlockDefrag();
        }
    }
}
