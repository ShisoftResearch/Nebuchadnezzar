package org.shisoft.neb.utils;

import org.github.jamm.MemoryMeter;

/**
 * Created by shisoft on 5/2/2016.
 */
public class MemoryMeasure {
    MemoryMeter meter = new MemoryMeter();

    public static long measure (Object object){
        MemoryMeasure mm = new MemoryMeasure();
        return mm.meter.measureDeep(object);
    }

}
