package org.shisoft.neb.io;

import clojure.lang.IFn;

/**
 * Created by shisoft on 21/1/2016.
 */
public class cellWriter {

    long startLoc;
    long currLoc;
    org.shisoft.neb.trunk trunk;

    private void init(org.shisoft.neb.trunk trunk, long length, long currLoc){
        this.trunk = trunk;
        this.currLoc = currLoc;
        this.startLoc = currLoc;
    }

    public cellWriter(org.shisoft.neb.trunk trunk, long length) {
        init(trunk, length, trunk.getPointer().getAndAdd(length));
    }

    public cellWriter(org.shisoft.neb.trunk trunk, long length, long currLoc){
        init(trunk, length, currLoc);
    }

    public void streamWrite (IFn fn, Object value, long length){
        fn.invoke(trunk, value, currLoc);
        currLoc += length;
    }

    public void addCellToTrunkIndex(long hash){
        trunk.getCellIndex().put(hash, new cellMeta(startLoc));
    }

    public void updateCellToTrunkIndex(long hash){
        trunk.getCellIndex().get(hash).setLocation(startLoc);
    }

    public long getCurrLoc() {
        return currLoc;
    }
}
