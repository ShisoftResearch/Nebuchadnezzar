package org.shisoft.neb.io;

/**
 * Created by shisoft on 21/1/2016.
 */

import clojure.lang.IFn;
import org.shisoft.neb.Trunk;

public class CellReader {

    long currLoc;
    Trunk trunk;

    public CellReader(Trunk trunk, long currLoc) {
        this.currLoc = currLoc;
        this.trunk = trunk;
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