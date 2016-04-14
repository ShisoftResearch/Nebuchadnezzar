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

    public CellWriter(Trunk trunk, long length, CellMeta meta) throws Exception {
        tryAllocate(trunk, length, meta);
    }

    private void tryAllocate(Trunk trunk, long length, CellMeta meta){
        try {
            Long fragSpaceLoc = null;
            float fillRatio = trunk.computeFillRatio();
            trunk.checkShouldInSlowMode(fillRatio);
            if (fillRatio > 0.8) {
                //fragSpaceLoc = trunk.getDefrag().tryAcquireFromFrag(length, meta);
            }
            if (fragSpaceLoc != null) {
                init(trunk, length, fragSpaceLoc);
            } else {
                AtomicBoolean overflowed = new AtomicBoolean(false);
                long loc = trunk.tryAcquireFromAppendHeader(length, overflowed, meta);
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

    public void rollBack () {
        trunk.getDefrag().addFragment(startLoc, startLoc + length - 1);
    }

    public void updateCellToTrunkIndex(CellMeta meta){
        meta.setLocation(startLoc);
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
