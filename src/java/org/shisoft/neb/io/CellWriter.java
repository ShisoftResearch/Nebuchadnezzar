package org.shisoft.neb.io;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import org.shisoft.neb.Trunk;
import org.shisoft.neb.exceptions.StoreFullException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongUnaryOperator;

/**
 * Created by shisoft on 21/1/2016.
 */
public class CellWriter {

    static IFn defragFn = Clojure.var("neb.defragment", "scan-trunk-and-defragment");
    long startLoc;
    long currLoc;
    long length;
    Trunk trunk;

    private void init(Trunk trunk, long length, long currLoc){
        this.trunk = trunk;
        this.currLoc = currLoc;
        this.startLoc = currLoc;
        this.length = length;
        trunk.copyMemForFork(startLoc, startLoc + length -1);
    }

    public CellWriter(Trunk trunk, long length) throws Exception {
        tryAllocate(trunk, length);
    }

    private void tryAllocate(Trunk trunk, long length){
        try {
            long trunkSize = trunk.getSize();
            long appendHeader = trunk.getAppendHeaderValue();
            float filledRatio = appendHeader / trunkSize;
            Long fragSpaceLoc = null;
            if (filledRatio > 0.8) {
                fragSpaceLoc = trunk.getDefrag().tryAcquireFromFrag(length);
            }
            if (fragSpaceLoc != null) {
                init(trunk, length, fragSpaceLoc);
            } else {
                AtomicBoolean overflowed = new AtomicBoolean(false);
                long loc = trunk.tryAcquireFromAppendHeader(length, overflowed);
                if (overflowed.get()){
                    throw new StoreFullException("Expected length:" + length + " remains:" + (trunk.getSize() - loc));
                }  else {
                    init(trunk, length, loc);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public CellWriter(Trunk trunk, long length, long currLoc){
        init(trunk, length, currLoc);
    }

    public void streamWrite (IFn fn, Object value, long length){
        fn.invoke(trunk, value, currLoc);
        currLoc += length;
    }

    public void addCellToTrunkIndex(long hash){
        HashLongObjMap<CellMeta> index = trunk.getCellIndex();
        synchronized (index) {
            index.put(hash, new CellMeta(startLoc));
        }
    }

    public void rollBack () {
        trunk.getDefrag().addFragment(startLoc, startLoc + length - 1);
    }

    public void updateCellToTrunkIndex(long hash){
        trunk.getCellIndex().get(hash).setLocation(startLoc);
    }

    public long getCurrLoc() {
        return currLoc;
    }

    public Trunk getTrunk() {
        return trunk;
    }

    public void markDirty () {
        trunk.addDirtyRanges(startLoc, currLoc - 1);
    }

    public long getStartLoc() {
        return startLoc;
    }
}
