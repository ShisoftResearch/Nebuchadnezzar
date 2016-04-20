package org.shisoft.neb.durability;

import org.shisoft.neb.Trunk;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by shisoft on 16-4-18.
 */
public class BackupCache {

    static final int maxQueueItems = 10;
    long msgId = 0;
    ConcurrentSkipListMap cacheQueue = new ConcurrentSkipListMap<>(Comparator.comparing(TrunkSegmentIdentifier::getMsgId));

    public void offer (int sid, int trunkId, int segId, Object data) {
        TrunkSegmentIdentifier id = new TrunkSegmentIdentifier(msgId, sid, trunkId, segId);
        while (!cacheQueue.containsKey(id) && cacheQueue.size() > maxQueueItems) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        cacheQueue.put(id, data);
        msgId++;
    }

    public Object pop () {
        Map.Entry e = cacheQueue.pollFirstEntry();
        if (e != null) {
            return e.getValue();
        } else {
            return null;
        }
    }

}
