package org.shisoft.neb;

import clojure.lang.IFn;
import org.shisoft.neb.utils.UnsafeUtils;

import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by shisoft on 16-3-25.
 */
public class MemoryFork {
    Trunk trunk;
    TreeMap<Long, byte[]> orignalBytes = new TreeMap<>();

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

    final static int segLength = 128 * 1024; // 32K segment

    // start and end are relative address
    public void syncBytes(long start, long end, IFn syncFn){
        synchronized (orignalBytes) {
            NavigableMap<Long, byte[]> es = orignalBytes.subMap(start, true, start, true);
            Map.Entry<Long, byte[]> e = new Map.Entry<Long, byte[]>() {
                public Long getKey() {
                    return -1L;
                }
                public byte[] getValue() {
                    return new byte[0];
                }
                public byte[] setValue(byte[] value) {
                    return new byte[0];
                }
            };
            long currentPos = start;
            while (currentPos <= end) {
                long bufferBound = Long.MAX_VALUE;
                if (e != null && e.getKey() < currentPos) {
                    e = es.ceilingEntry(currentPos);
                }
                if (e != null && e.getKey() == currentPos) {
                    syncFn.invoke(e.getValue(), currentPos);
                    currentPos += e.getValue().length;
                    continue;
                }
                if (e != null) {
                    bufferBound = e.getKey();
                }
                long segEnd = Math.min(Math.min(start + segLength - 1, bufferBound - 1), end);
                byte[] bs = UnsafeUtils.getBytes(
                        trunk.getStoreAddress() + currentPos,
                        (int) (segEnd - currentPos + 1)
                );
                if (bs.length > 0) {
                    syncFn.invoke(bs, currentPos);
                    currentPos += bs.length;
                } else {
                    return;
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
