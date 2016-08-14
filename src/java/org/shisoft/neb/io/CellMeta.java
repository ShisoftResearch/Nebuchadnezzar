package org.shisoft.neb.io;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;

/**
 * Created by shisoft on 5/2/2016.
 */
public class CellMeta {

    private volatile long location;

    public CellMeta(long location) {
        this.location = location;
    }

    public long getLocation() {
        return location;
    }

    public void setLocation(long location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "CellMeta{" +
                "location=" + location +
                '}';
        ConcurrentHashMap
    }
}
