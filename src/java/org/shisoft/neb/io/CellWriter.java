package org.shisoft.neb.io;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.shisoft.neb.Trunk;
import org.shisoft.neb.exceptions.StoreFullException;

/**
 * Created by shisoft on 21/1/2016.
 */
public class CellWriter {

    static IFn defragFn = Clojure.var("neb.defragment", "scan-trunk-and-defragment");
    long startLoc;
    long currLoc;
    Trunk trunk;

    private void init(Trunk trunk, long length, long currLoc){
        this.trunk = trunk;
        this.currLoc = currLoc;
        this.startLoc = currLoc;
        trunk.copyMemForFork(startLoc, startLoc + length -1);
    }

    public CellWriter(Trunk trunk, long length) throws Exception {
        tryAllocate(trunk, length);
    }

    public void tryAllocate(Trunk trunk, long length){
        try {
            long loc = trunk.getAppendHeader().getAndAdd(length);
            if (loc + length > trunk.getSize()){
                trunk.getAppendHeader().set(loc);
                throw new StoreFullException("Expected length:" + length + " remains:" + (trunk.getSize() - loc));
            }  else {
                init(trunk, length, loc);
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
        trunk.getCellIndex().put(hash, new CellMeta(startLoc));
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
