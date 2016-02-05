package org.shisoft.neb.io;

import clojure.lang.IFn;

/**
 * Created by shisoft on 21/1/2016.
 */
public class cellWriter {

    int startLoc;
    int currLoc;
    org.shisoft.neb.trunk trunk;

    private void init(org.shisoft.neb.trunk trunk, int length, int currLoc){
        this.trunk = trunk;
        this.currLoc = currLoc;
        this.startLoc = currLoc;
    }

    public cellWriter(org.shisoft.neb.trunk trunk, int length) {
        init(trunk, length, trunk.getPointer().getAndAdd(length));
    }

    public cellWriter(org.shisoft.neb.trunk trunk, int length, int currLoc){
        init(trunk, length, currLoc);
    }

    public void streamWrite (IFn fn, Object value, int length){
        fn.invoke(trunk, value, currLoc);
        currLoc += length;
    }

    public void addCellToTrunkIndex(int hash){
        trunk.getCellIndex().put(hash, new cellMeta(startLoc));
    }

    public void updateCellToTrunkIndex(int hash){
        trunk.getCellIndex().get(hash).setLocation(startLoc);
    }

    public int getCurrLoc() {
        return currLoc;
    }
}
