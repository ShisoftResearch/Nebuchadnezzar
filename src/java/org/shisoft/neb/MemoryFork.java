package org.shisoft.neb;

import clojure.lang.IFn;
import org.shisoft.neb.utils.UnsafeUtils;

import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiFunction;

/**
 * Created by shisoft on 16-3-25.
 */
public class MemoryFork {
    Trunk trunk;
    ConcurrentSkipListMap<Long, byte[]> orignalBytes = new ConcurrentSkipListMap<>();

    public MemoryFork(Trunk trunk) {
        this.trunk = trunk;
    }

    public void copyMemory (long start, long end){
        long bsLen = end - start;
        byte[] bs = new byte[(int) bsLen];
        for (long i = start; i < end; i++) {
            bs[(int) (i - start)] = Trunk.getUnsafe().getByte(trunk.getStoreAddress() + i);
        }
        orignalBytes.compute(start, (aLong, saved) -> {
            if (saved != null && saved.length >= (end - start + 1)) {
                return saved;
            } else {
                return bs;
            }
        });
    }

    final static int segLength = 2 * 1024 * 1024; // 2M segment

    // start and end are relative address
    public synchronized void syncBytes(long start, long end, IFn syncFn){
        assert start <= end;
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

    public synchronized void release(){
        orignalBytes.clear();
        trunk.setMemoryFork(null);
    }
}
