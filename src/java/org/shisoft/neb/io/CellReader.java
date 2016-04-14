package org.shisoft.neb.io;

/**
 * Created by shisoft on 21/1/2016.
 */

import clojure.lang.IFn;
import org.shisoft.neb.Trunk;
import org.shisoft.neb.exceptions.CellFormatErrorException;

public class CellReader {

    long currLoc;
    Trunk trunk;

    public CellReader(Trunk trunk, long currLoc) throws CellFormatErrorException {
        this.currLoc = currLoc;
        this.trunk = trunk;
        byte tag = Reader.readByte(trunk, currLoc);
        if (tag == 1) { // cell type field, 0 for cell, 1 for tombstone
            throw new CellFormatErrorException("is a tombstone, for trunk: " + String.valueOf(trunk.getId()));
        } else if (tag != 0) {
            throw new CellFormatErrorException("for trunk:  " + String.valueOf(trunk.getId()));
        }
    }

    public Object streamRead (IFn fn){
        return fn.invoke(trunk, currLoc);
    }

    public Object streamRead (IFn fn, long len){
        Object r = streamRead(fn);
        advancePointer(len);
        return r;
    }

    public long advancePointer(long len){
        long originalLoc = currLoc;
        currLoc += len;
        return originalLoc;
    }

    public long getCurrLoc() {
        return currLoc;
    }
}
