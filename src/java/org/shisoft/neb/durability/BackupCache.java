package org.shisoft.neb.durability;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by shisoft on 16-4-18.
 */
public class BackupCache {
    ConcurrentSkipListMap<TrunkSegmentIdentifier, Object> cacheQueue = new ConcurrentSkipListMap<>();

    public void offer (int sid, int trunkId, int segId, Object data) {
        cacheQueue.put(new TrunkSegmentIdentifier(sid, trunkId, segId), data);
    }

    public Map.Entry<TrunkSegmentIdentifier, Object> pop () {
        return cacheQueue.pollFirstEntry();
    }

}
