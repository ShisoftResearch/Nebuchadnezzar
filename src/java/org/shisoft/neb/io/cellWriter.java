package org.shisoft.neb.io;

import clojure.lang.IFn;

/**
 * Created by shisoft on 21/1/2016.
 */
public class cellWriter {

    int startLoc;
    int currLoc;
    org.shisoft.neb.trunk trunk;

    public cellWriter(org.shisoft.neb.trunk trunk, int length) {
        this.trunk = trunk;
        this.currLoc = trunk.getPointer().getAndAdd(length);
        this.startLoc = currLoc;
    }

    public void streamWrite (IFn fn, Object value, int length){
        fn.invoke(trunk, value, currLoc);
        currLoc += length;
    }

    public void addCellToTrunkIndex(int hash){
        trunk.getCellIndex().addValue(hash, startLoc);
    }

    public int getCurrLoc() {
        return currLoc;
    }
}
