package org.shisoft.neb;

import org.shisoft.neb.io.CellMeta;
import org.shisoft.neb.utils.Bindings;
import org.shisoft.neb.utils.Collection;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongUnaryOperator;

/**
 * Created by shisoft on 16-4-11.
 */
public class Defragmentation {
    private volatile long currentDefragLoc = 0;
    private Trunk trunk;
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

    public Map.Entry<Long, Long> addFragment_(long startPos, long endPos) {
        return Collection.addAndAutoMerge(fragments, startPos, endPos);
    }
    public void putTombstone (Map.Entry<Long, Long> actualRange) {
        if (actualRange != null) {
            trunk.putTombstone(actualRange.getKey(), actualRange.getValue());
        }
    }
    public void addFragment (long startPos, long endPos) {
        opLock.lock();
        Map.Entry<Long, Long> actualRange = addFragment_(startPos, endPos);
        putTombstone(actualRange);
        opLock.unlock();
    }
    public void defrag (){
        lockDefrag();
        AtomicBoolean headerMoved = new AtomicBoolean(false);
        try {
            if (fragments.isEmpty()) return;
            long lwPos, hnPos;
            long currentAppendHeader;
            while (true) {
                try {
                    opLock.lock();
                    final Map.Entry<Long, Long> frag = fragments.ceilingEntry(currentDefragLoc);
                    if (frag == null) break;
                    final long hiPos = frag.getValue();
                    lwPos = frag.getKey();hnPos = hiPos + 1;
                    if (!fragments.remove(lwPos, hiPos)) continue;
                    currentAppendHeader = trunk.getAppendHeader().updateAndGet(appendHeader -> {
                        if (appendHeader == hiPos + 1) {
                            headerMoved.set(true);
                            return frag.getKey();
                        } else {
                            return appendHeader;
                        }
                    });
                    if (currentAppendHeader != frag.getKey() && frag.getKey() < currentAppendHeader) {
                        long cellHash = (long) Bindings.readCellHash.invoke(trunk, hnPos);
                        long cellLen = (int) Bindings.readCellLength.invoke(trunk, hnPos);
                        cellLen += cellHeadLen;
                        CellMeta meta = trunk.getCellIndex().get(cellHash);
                        if (meta != null) {
                            currentDefragLoc = lwPos + cellLen;
                            Map.Entry<Long, Long> fragRange = addFragment_(currentDefragLoc, hnPos + cellLen -1);
                            opLock.unlock();
                            boolean cellUpdated = false;
                            synchronized (meta) {
                                if (meta.getLocation() == hnPos) {
                                    trunk.copyMemory(hnPos, lwPos, cellLen);
                                    meta.setLocation(lwPos);
                                    cellUpdated = true;
                                }
                            }
                            putTombstone(fragRange);
                            if (!cellUpdated) {
                                //System.out.println("WARNING: Cell changed when defrag " + cellHash);
                                addFragment(lwPos, currentDefragLoc);
                            }
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                } finally {
                    if (opLock.isHeldByCurrentThread()) {
                        opLock.unlock();
                    }
                }
            }
            currentDefragLoc = 0;
            if (!headerMoved.get()) {
                System.out.println("WARNING: Defrag finished without moving header");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            unlockDefrag();
        }
    }
    public Long tryAcquireFromFrag(long size){
        try {
            opLock.lock();
            Map.Entry<Long, Long> frag = null;
            long requiredSize = size + Trunk.tombstoneSize;
            long totalFragSize = 0;
            for (Map.Entry<Long, Long> fragc : fragments.entrySet()) {
                long fsize = fragc.getValue() - fragc.getKey();
                if (fsize == size || fsize >= requiredSize) {
                    frag = fragments.floorEntry(fragc.getKey());
                    break;
                }
                totalFragSize += fsize;
            }
//            if (frag == null && totalFragSize >= requiredSize) {
//                opLock.unlock();
//                defrag();
//                try {
//                    return tryAcquireFromFrag(size);
//                } catch (StackOverflowError se) {
//                    System.out.println("Failed to acquire frag after defrag due to frequent changes");
//                    return null;
//                }
//            }
            if (frag != null) {
                long start = frag.getKey();
                long end = frag.getValue();
                long fragSize = end - start;
                if (fragSize != size && fragSize < requiredSize) {
                    return null;
                } else {
                    if (fragments.remove(start, end)) {
                        if (fragSize != size){
                            long newFragStart = start + size;
                            addFragment(newFragStart, end);
                        }
                        return start;
                    } else {
                        try {
                            return tryAcquireFromFrag(size);
                        } catch (StackOverflowError se) {
                            System.out.println("Failed to acquire frag due to frequent changes");
                            return null;
                        }
                    }
                }
            } else {
                return null;
            }
        } finally {opLock.unlock();}
    }
}
