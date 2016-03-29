package org.shisoft.neb.io;

/**
 * Created by shisoft on 5/2/2016.
 */
public class CellMeta {

    long location;

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
    }
}
