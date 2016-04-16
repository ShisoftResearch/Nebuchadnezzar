package org.shisoft.neb.io;

import org.shisoft.neb.Trunk;

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

    public void setLocation(long location, Trunk trunk) {
        assert location >= trunk.getStoreAddress() && location < trunk.getStoreAddress() + trunk.getSize();
        this.location = location;
    }

    @Override
    public String toString() {
        return "CellMeta{" +
                "location=" + location +
                '}';
    }
}
