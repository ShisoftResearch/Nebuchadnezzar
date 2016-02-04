package org.shisoft.neb.io;

/**
 * Created by shisoft on 21/1/2016.
 */

import clojure.lang.IFn;
import org.shisoft.neb.trunk;

public class cellReader {

    int currLoc;
    trunk trunk;

    public cellReader(org.shisoft.neb.trunk trunk, int currLoc) {
        this.currLoc = currLoc;
        this.trunk = trunk;
    }

    public Object streamRead (IFn fn, int len){
        Object r = fn.invoke(trunk, currLoc);
        advancePointer(len);
        return r;
    }

    public int advancePointer(int len){
        int originalLoc = currLoc;
        currLoc += len;
        return originalLoc;
    }

    public int getCurrLoc() {
        return currLoc;
    }
}
