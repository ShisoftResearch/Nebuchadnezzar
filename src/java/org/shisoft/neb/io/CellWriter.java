package org.shisoft.neb.io;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.shisoft.neb.Trunk;

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
        tryAllocate(trunk, length, false);
    }

    public void tryAllocate(Trunk trunk, long length, boolean defraged){
        trunk.getCellWriterLock().lock();
        try {
            long loc = trunk.getAppendHeader().getAndAdd(length);
            if (loc + length > trunk.getSize()){
                trunk.getAppendHeader().set(loc);
                if (defraged){
                    throw new Exception("Store full, expected length:" + length + " remains:" + (trunk.getSize() - loc));
                } else {
                    defragFn.invoke(trunk);
                    tryAllocate(trunk, length, true);
                }
            }  else {
                init(trunk, length, loc);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            trunk.getCellWriterLock().unlock();
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
