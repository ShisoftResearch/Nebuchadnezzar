package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashLongByteMap;
import net.openhft.koloboke.collect.map.hash.HashLongByteMaps;

/**
 * Created by shisoft on 16-3-25.
 */
public class MemoryFork {
    Trunk trunk;
    HashLongByteMap orignalBytes = HashLongByteMaps.newMutableMap();

    public MemoryFork(Trunk trunk) {
        this.trunk = trunk;
    }

    public void copyMemory (long start, long end){
        for (long i = start; i < end; i++){
            orignalBytes.putIfAbsent(i, Trunk.getUnsafe().getByte(i));
        }
    }

    public byte getByte(long pos){
        return orignalBytes.getOrDefault(pos, Trunk.getUnsafe().getByte(pos));
    }

    public void release(){
        orignalBytes.clear();
        trunk.memoryFork = null;
    }
}
