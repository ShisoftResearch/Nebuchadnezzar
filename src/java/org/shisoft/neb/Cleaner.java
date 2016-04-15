package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import org.shisoft.neb.io.CellMeta;
import org.shisoft.neb.io.Reader;
import org.shisoft.neb.io.type_lengths;
import org.shisoft.neb.utils.Bindings;
import org.shisoft.neb.utils.Collection;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Created by shisoft on 16-4-11.
 */
public class Cleaner {

    private Trunk trunk;
    private long cellHeadLen = (long) Bindings.cellHeadLen.invoke();
    private final ConcurrentSkipListMap<Long, Long> fragments = new ConcurrentSkipListMap<>();
    public Cleaner(Trunk trunk) {
        this.trunk = trunk;
    }


    public void addFragment (long startPos, long endPos) {
        Segment seg = trunk.locateSegment(startPos);
        seg.getLock().writeLock().lock();
        try {
            seg.getFrags().add(startPos);
            seg.incDeadObjectBytes((int) (endPos - startPos + 1));
            trunk.putTombstone(startPos, endPos);
        } finally {
            seg.getLock().writeLock().unlock();
        }
    }

    public void phaseOneCleaning () {
        Arrays.stream(trunk.getSegments()).parallel()
                .sorted((o1, o2) -> Float.valueOf(o1.aliveDataRatio()).compareTo(o2.aliveDataRatio()))
                .limit((long) (trunk.getSegments().length * 0.5))
                .forEach(segment -> {
                    long pos = segment.getBaseAddr();
                    while (true) {
                        segment.getLock().writeLock().lock();
                        try {
                            Long fragLoc = segment.getFrags().ceiling(pos);
                            if (fragLoc != null && Reader.readByte(fragLoc) == 1) {
                                int fragLen = Reader.readInt(fragLoc + type_lengths.byteLen);
                                long adjPos = fragLoc + fragLen;
                                segment.getFrags().remove(fragLoc);
                                if (adjPos == segment.getCurrentLoc()) {
                                    if (!segment.resetCurrentLoc(adjPos, fragLoc)) {
                                        System.out.println("Seg curr pos moved");
                                    } else {
                                        segment.decDeadObjectBytes(fragLen);
                                        if (segment.getDeadObjectBytes() != 0) {
                                            System.out.println("Segment is not totally clean");
                                        }
                                    }
                                } else {
                                    if (Reader.readByte(adjPos) == 0) {
                                        long cellHash = (long) Bindings.readCellHash.invoke(trunk, adjPos);
                                        CellMeta meta = trunk.getCellIndex().get(cellHash);
                                        if (meta != null) {
                                            synchronized (meta) {
                                                if (meta.getLocation() == adjPos) {
                                                    int cellLen = (Integer) Bindings.readCellLength.invoke(trunk, adjPos);
                                                    cellLen += cellHeadLen;
                                                    trunk.copyMemory(adjPos, fragLoc, cellLen);
                                                    meta.setLocation(fragLoc);
                                                    addFragment(fragLoc + cellLen, fragLoc + fragLen - 1);
                                                    pos = fragLoc + cellLen;
                                                } else {
                                                    System.out.println("Cell meta modified in frag adj");
                                                }
                                            }
                                        } else {
                                            System.out.println("Cell cannot been found in frag adj");
                                        }
                                    } else if (Reader.readByte(adjPos) == 1) {
                                        if (segment.getFrags().contains(adjPos)) {
                                            int adjFragLen = Reader.readInt(adjPos + type_lengths.byteLen);
                                            segment.getFrags().remove(adjPos);
                                            addFragment(fragLoc, adjPos + adjFragLen - 1);
                                        } else {
                                            System.out.println("Adj frag does not on record");
                                        }
                                    } else {
                                        System.out.println("Adj pos cannot been recognized");
                                    }
                                }
                            } else {
                                System.out.println("Location is not a frag");
                            }
                        } finally {
                            segment.getLock().writeLock().unlock();
                        }
                    }
                });
    }

    public void phaseTwoCleaning () {

    }

    public void clean () {
        phaseOneCleaning();
    }

}
