package org.shisoft.neb.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by shisoft on 16-4-11.
 */
public class Collection {

    private static class EntryChangedException extends IllegalStateException {}

    public static Map.Entry<Long, Long> addAndAutoMerge_ (ConcurrentSkipListMap<Long, Long> mapToMerge, final long startPos, final long endPos) {
        Map.Entry<Long, Long> prevPair = mapToMerge.lowerEntry(startPos);
        Map.Entry<Long, Long> forPair = mapToMerge.higherEntry(startPos);
        Long dupLoc = mapToMerge.get(startPos);
        if (dupLoc != null && dupLoc >= endPos) return null;
        if (prevPair != null && prevPair.getValue() >= endPos) return null;
        if (prevPair != null && (prevPair.getValue() >= startPos || prevPair.getValue() == startPos - 1)) {
            return addAndAutoMerge(mapToMerge, prevPair.getKey(), endPos);
        } else if (forPair != null && (forPair.getKey() < endPos || forPair.getKey() == endPos + 1)) {
            if (!mapToMerge.remove(forPair.getKey(), forPair.getValue())) {throw new EntryChangedException();}
            if (forPair.getValue() < endPos){
                return addAndAutoMerge(mapToMerge, startPos, endPos);
            } else {
                return addAndAutoMerge(mapToMerge, startPos, forPair.getValue());
            }
        } else {
            mapToMerge.put(startPos, endPos);
            return new Map.Entry<Long, Long>() {
                public Long getKey() {
                    return startPos;
                }
                public Long getValue() {
                    return endPos;
                }
                public Long setValue(Long value) {
                    return null;
                }
            };
        }
    }

    public static Map.Entry<Long, Long> addAndAutoMerge (ConcurrentSkipListMap<Long, Long> mapToMerge, final long startPos, final long endPos) {
        while (true) {
            try {
                return addAndAutoMerge_(mapToMerge, startPos, endPos);
            } catch (EntryChangedException ignored) {}
        }
    }
}
