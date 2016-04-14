package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import org.shisoft.neb.io.CellMeta;
import org.shisoft.neb.utils.Bindings;
import org.shisoft.neb.utils.Collection;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

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
    public void rebuildFrags() {
        HashLongObjMap<CellMeta> index = trunk.getCellIndex();
        trunk.getCellWriteLock().writeLock().lock();
        opLock.lock();
        this.fragments.clear();
        AtomicLong pos = new AtomicLong(0);
        long originalHeader = trunk.getAppendHeaderValue();
        try {
            trunk.getCellIndex().values().stream()
                    .mapToLong(CellMeta::getLocation)
                    .sorted()
                    .filter(loc -> loc > 0)
                    .forEachOrdered(starts -> {
                        long hash = (long) Bindings.readCellHash.invoke(trunk, starts);
                        CellMeta meta = index.get(hash);
                        if (meta != null && meta.getLocation() == starts) {
                            synchronized (meta) {
                                long len = (int) Bindings.readCellLength.invoke(trunk, starts) + cellHeadLen;
                                long ends = starts + len - 1;
                                long currPos = pos.get();
                                assert currPos <= starts;
                                if (currPos != starts) {
                                    addFragment(currPos, starts - 1);
                                }
                                pos.set(ends + 1);
                            }
                        } else {
                            System.out.println("WARNING: Meta is not stable");
                        }
                    });
            assert trunk.getAppendHeader().compareAndSet(originalHeader, pos.get());
            System.out.println("Append header reset to " + pos.get() + " for " + trunk.getId());
        } finally {
            opLock.unlock();
            trunk.getCellWriteLock().writeLock().unlock();
        }
    }
    public void defrag (){
        lockDefrag();
        AtomicBoolean headerMoved = new AtomicBoolean(false);
        Map.Entry<Long, Long> lastFrag = null;
        try {
            if (fragments.isEmpty()) return;
            long lwPos, hnPos;
            long currentAppendHeader = 0;
            while (true) {
                try {
                    trunk.checkShouldInSlowMode();
                    opLock.lock();
                    final Map.Entry<Long, Long> frag = fragments.ceilingEntry(currentDefragLoc);
                    if (frag == null) break;
                    lastFrag = frag;
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
                        opLock.unlock();
                        Long cellLen = null;
                        CellMeta meta = trunk.getCellIndex().get(cellHash);
                        if (meta != null) {
                            boolean cellUpdated = false;
                            synchronized (meta) {
                                if (meta.getLocation() == hnPos) {
                                    cellLen = ((Integer) Bindings.readCellLength.invoke(trunk, hnPos)).longValue();
                                    assert cellLen > 0;
                                    cellLen += cellHeadLen;
                                    trunk.copyMemory(hnPos, lwPos, cellLen);
                                    meta.setLocation(lwPos);
                                    cellUpdated = true;
                                }
                            }
                            if (cellUpdated) {
                                assert cellLen != null;
                                currentDefragLoc = lwPos + cellLen;
                                addFragment(currentDefragLoc, hnPos + cellLen -1);
                                continue;
                            }
                        }
                        addFragment(lwPos, hiPos);
                        currentDefragLoc = hiPos;
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
                assert lastFrag != null;
                System.out.println("WARNING:s Defrag finished without moving header " +
                        lastFrag.getKey() + "," + lastFrag.getValue() + " " +
                        currentAppendHeader + " starting rebuild for " + trunk.getId()
                );
                rebuildFrags();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            unlockDefrag();
        }
    }

    public boolean checkFragSpace (long expected) {
        return expected <= fragments.entrySet().stream()
                .mapToLong(value -> value.getValue() - value.getKey())
                .reduce((left, right) -> left + right).getAsLong();
    }

    public Long tryAcquireFromFrag(long size, CellMeta meta){
        try {
            opLock.lock();
            Map.Entry<Long, Long> frag = null;
            long requiredSize = size + Trunk.tombstoneSize;
            long totalFragSize = 0;
            for (Map.Entry<Long, Long> fragc : fragments.entrySet()) {
                long fsize = fragc.getValue() - fragc.getKey();
                if (fsize == size || fsize >= requiredSize) {
                    frag = fragc;
                    break;
                }
                totalFragSize += fsize;
            }
            if (frag != null) {
                if (!fragments.remove(frag.getKey(), frag.getValue())) {
                    return null;
                }
                long start = frag.getKey();
                long end = frag.getValue();
                long fragSize = end - start;
                if (fragSize != size){
                    long newFragStart = start + size;
                    addFragment(newFragStart, end);
                }
                if (meta != null && meta.getLocation() < 0) {
                    meta.setLocation(start);
                }
                return start;
            } else {
                return null;
            }
        } finally {
            opLock.unlock();
        }
    }
}
