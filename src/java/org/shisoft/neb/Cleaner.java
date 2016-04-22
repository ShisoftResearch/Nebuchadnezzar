package org.shisoft.neb;

import org.shisoft.neb.exceptions.MemoryOutOfBoundException;
import org.shisoft.neb.io.CellMeta;
import org.shisoft.neb.io.Reader;
import org.shisoft.neb.io.type_lengths;
import org.shisoft.neb.utils.Bindings;

import java.util.Arrays;
import java.util.concurrent.ConcurrentSkipListMap;

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
        seg.lockWrite();
        try {
            assert startPos >= seg.getBaseAddr() && endPos < seg.getBaseAddr() + Trunk.getSegSize();
            seg.getFrags().add(startPos);
            seg.incDeadObjectBytes((int) (endPos - startPos + 1));
            trunk.putTombstone(startPos, endPos);
        } finally {
            seg.unlockWrite();
        }
    }

    public void removeFragment (Segment seg, long startPos, int length) {
        seg.lockWrite();
        try {
            assert startPos >= seg.getBaseAddr() && length + startPos - 1 < seg.getBaseAddr() + Trunk.getSegSize();
            seg.getFrags().remove(startPos);
            seg.decDeadObjectBytes(length);
            //seg.appendTombstones(startPos);
        } finally {
            seg.unlockWrite();
        }
    }

    private void checkTooManyRetry(String message, int retry) {
        if (retry > 1000) {
            System.out.println(message + " " + retry);
        }
    }

    private void phaseOneCleanSegment(Segment segment) {
        long pos = segment.getBaseAddr();
        int retry = 0;
        while (true) {
            try {
                if (segment.getFrags().isEmpty()) break;
                segment.lockWrite();
                Long fragLoc = segment.getFrags().ceiling(pos);
                if (fragLoc == null) break;
                if (fragLoc < segment.getBaseAddr() || fragLoc >= segment.getBaseAddr() + Trunk.getSegSize()) {
                    System.out.println("Frag out of segment range: " + segment.getBaseAddr() + " ~ " + (segment.getBaseAddr() + Trunk.getSegSize()) + ", " + fragLoc);
                    segment.getFrags().remove(fragLoc);
                    break;
                }
                if (Reader.readByte(fragLoc) == 2) {
                    int fragLen = Reader.readInt(fragLoc + type_lengths.byteLen);
                    long adjPos = fragLoc + fragLen;
                    if (adjPos >= segment.getBaseAddr() + Trunk.getSegSize() ||
                            adjPos >= trunk.getStoreAddress() + trunk.getSize() ||
                            adjPos < segment.getBaseAddr() ||
                            adjPos < trunk.getStoreAddress()) {
                        System.out.println("out of boundary in cleaner, will ignore frag");
                        segment.getFrags().remove(fragLoc);
                        break;
                    }
                    if (adjPos == segment.getCurrentLoc()) {
                        if (!segment.resetCurrentLoc(adjPos, fragLoc)) {
                            retry++;
                            checkTooManyRetry("Seg curr pos moved", retry);
                        } else {
                            removeFragment(segment, fragLoc, fragLen);
                            retry = 0;
                        }
                    } else {
                        if (Reader.readByte(adjPos) == 1) {
                            long cellHash = (long) Bindings.readCellHash.invoke(trunk, adjPos);
                            CellMeta meta = trunk.getCellIndex().get(cellHash);
                            if (meta != null) {
                                segment.unlockWrite();
                                synchronized (meta) {
                                    if (meta.getLocation() == adjPos) {
                                        int cellLen = (int) Bindings.readCellLength.invoke(trunk, adjPos);
                                        cellLen += cellHeadLen;
                                        trunk.copyMemory(adjPos, fragLoc, cellLen);
                                        meta.setLocation(fragLoc);
                                        removeFragment(segment, fragLoc, fragLen);
                                        addFragment(fragLoc + cellLen, adjPos + cellLen - 1);
                                        pos = fragLoc + cellLen;
                                        retry = 0;
                                    } else {
                                        retry++;
                                        //checkTooManyRetry("Cell meta modified in frag adj", retry);
                                    }
                                }
                            } else {
                                retry++;
                                //checkTooManyRetry("Cell cannot been found in frag adj", retry);
                            }
                        } else if (Reader.readByte(adjPos) == 2) {
                            if (segment.getFrags().contains(adjPos)) {
                                int adjFragLen = Reader.readInt(adjPos + type_lengths.byteLen);
                                removeFragment(segment, fragLoc, fragLen);
                                removeFragment(segment, adjPos, adjFragLen);
                                addFragment(fragLoc, adjPos + adjFragLen - 1);
                                retry = 0;
                            } else {
                                retry++;
                                checkTooManyRetry("Adj frag does not on record", retry);
                            }
                        } else {
                            retry++;
//                            checkTooManyRetry("Adj pos cannot been recognized " + Reader.readByte(adjPos) + " " +
//                                    pos + " " + segment.getCurrentLoc(), retry);
                        }
                    }
                } else {
                    retry++;
                    checkTooManyRetry("Location is not a frag", retry);
                }
            } finally {
                segment.unlockWrite();
            }
        }
    }

    public void phaseOneCleaning () {
        Arrays.stream(trunk.getSegments())
                .filter(seg -> seg.getFrags().size() > 0)
                //.sorted((o1, o2) -> Integer.valueOf(o2.getDeadObjectBytes()).compareTo(o1.getDeadObjectBytes()))
                //.limit((long) (Math.max(1, trunk.getSegments().length * 0.5)))
                .parallel()
                .forEach(this::phaseOneCleanSegment);
    }

    public void phaseTwoCleaning () {

    }

    public void clean () {
        phaseOneCleaning();
    }

}
