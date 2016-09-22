package org.shisoft.neb.io;

/**
 * Created by shisoft on 21/1/2016.
 */

import clojure.lang.IFn;
import org.shisoft.neb.Trunk;
import org.shisoft.neb.exceptions.CellFormatErrorException;
import org.shisoft.neb.exceptions.MemoryOutOfBoundException;

public class CellReader {

    long currLoc;
    Trunk trunk;

    public CellReader(Trunk trunk, long currLoc) throws CellFormatErrorException, MemoryOutOfBoundException {
        if (currLoc >= trunk.getStoreAddress() + trunk.getSize() || currLoc < trunk.getStoreAddress()) {
            throw new MemoryOutOfBoundException(trunk.getStoreAddress() + " " + trunk.getSize() + " " +
                    String.valueOf(trunk.getStoreAddress() + trunk.getSize()) + " " + currLoc);
        }
        this.currLoc = currLoc;
        this.trunk = trunk;
        byte tag = Reader.readByte(currLoc);
        if (tag == 2) { // cell type field, 1 for cell, 2 for tombstone
            throw new CellFormatErrorException("is a tombstone, for trunk: " + String.valueOf(trunk.getId()));
        } else if (tag != 1) {
            throw new CellFormatErrorException("for trunk: " + String.valueOf(trunk.getId()) + ", tag: " + tag);
        }
    }

    public Object streamRead (IFn fn){
        return fn.invoke(currLoc);
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
