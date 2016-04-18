package org.shisoft.neb.durability;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by shisoft on 16-4-18.
 */
public class BackupCache {

    long msgId = 0;
    ConcurrentSkipListMap cacheQueue = new ConcurrentSkipListMap<>(Comparator.comparing(TrunkSegmentIdentifier::getMsgId));

    public void offer (int sid, int trunkId, int segId, Object data) {
        cacheQueue.put(new TrunkSegmentIdentifier(msgId, sid, trunkId, segId), data);
        msgId++;
    }

    public Object pop () {
        return cacheQueue.pollFirstEntry().getValue();
    }

}
