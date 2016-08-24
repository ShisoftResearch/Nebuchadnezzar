package org.shisoft.neb.io;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.shisoft.neb.Trunk;
import org.shisoft.neb.exceptions.ObjectTooLargeException;
import org.shisoft.neb.exceptions.StoreFullException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by shisoft on 21/1/2016.
 */
public class CellWriter {

    static IFn defragFn = Clojure.var("neb.defragment", "scan-trunk-and-defragment");
    long startLoc;
    AtomicLong currLoc;
    long length;
    Trunk trunk;

    private void init(Trunk trunk, long length, long currLoc){
        this.trunk = trunk;
        this.currLoc = new AtomicLong(currLoc);
        this.startLoc = currLoc;
        this.length = length;
    }

    public CellWriter(Trunk trunk, long length) throws Exception {
        tryAllocate(trunk, length);
    }

    private void tryAllocate(Trunk trunk, long length) throws ObjectTooLargeException, StoreFullException {
        long loc = trunk.tryAcquireSpace(length);
        if (loc < 0){
            throw new StoreFullException("Expected length:" + length);
        }  else {
            assert loc >= trunk.getStoreAddress();
            init(trunk, length, loc);
        }
    }

    public CellWriter(Trunk trunk, long length, long currLoc){
        init(trunk, length, currLoc);
    }

    public void streamWrite (IFn fn, Object value, long length){
        fn.invoke(value, currLoc);
        currLoc.addAndGet(length);
    }

    public void streamWrite (IFn fn, Object value, long length, long offset){
        fn.invoke(value, startLoc + offset);
        currLoc.addAndGet(length);
    }

    public void rollBack () {
        System.out.println("Rolling back for trunk: " + trunk.getId());
        trunk.getCleaner().addFragment(startLoc, startLoc + length - 1);
    }

    public long updateCellToTrunkIndex(long hash, Trunk trunk){
        synchronized (trunk.getCellIndex()) {
            trunk.getCellIndex().replace(hash, startLoc);
        }
        return startLoc;
    }

    public long addCellMetaToTrunkIndex(long hash, Trunk trunk) throws Exception {
        synchronized (trunk.getCellIndex()) {
            trunk.getCellIndex().put(hash, startLoc);
        }
        return startLoc;
    }

    public long getCurrLoc() {
        return currLoc.get();
    }

    public Trunk getTrunk() {
        return trunk;
    }

    public void markDirty () {
        trunk.addDirtyRanges(startLoc, currLoc.get() - 1);
    }

    public long getStartLoc() {
        return startLoc;
    }
}
