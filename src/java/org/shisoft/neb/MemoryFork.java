package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashLongByteMap;
import net.openhft.koloboke.collect.map.hash.HashLongByteMaps;
import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import org.shisoft.neb.utils.UnsafeUtils;

/**
 * Created by shisoft on 16-3-25.
 */
public class MemoryFork {
    Trunk trunk;
    HashLongObjMap<byte[]> orignalBytes = HashLongObjMaps.newMutableMap();

    public MemoryFork(Trunk trunk) {
        this.trunk = trunk;
    }

    public void copyMemory (long start, long end){
        synchronized (orignalBytes) {
            byte[] saved = orignalBytes.get(start);
            if (saved != null && saved.length >= (end - start + 1)) {
                return;
            }
            long bsLen = end - start;
            byte[] bs = new byte[(int) bsLen];
            for (long i = start; i < end; i++) {
                bs[(int) (i - start)] = Trunk.getUnsafe().getByte(i);
            }
            orignalBytes.put(start, bs);
        }
    }

    public byte[] getBytes(long start, long end){
        synchronized (orignalBytes) {
            byte[] saved = orignalBytes.get(start);
            int fetchLen = (int) (end - start + 1);
            if (saved == null) {
                return UnsafeUtils.getBytes(start, fetchLen);
            } else {
                if (saved.length >= fetchLen) {
                    return UnsafeUtils.subBytes(saved, 0, fetchLen);
                } else {
                    byte[] r = new byte[fetchLen];
                    byte[] t = UnsafeUtils.getBytes(start + saved.length, fetchLen - saved.length);
                    System.arraycopy(saved, 0, r, 0, saved.length);
                    System.arraycopy(t, 0, r, saved.length, t.length);
                    return r;
                }
            }
        }
    }

    public void release(){
        synchronized (orignalBytes){
            orignalBytes.clear();
            trunk.setMemoryFork(null);
        }
    }
}
