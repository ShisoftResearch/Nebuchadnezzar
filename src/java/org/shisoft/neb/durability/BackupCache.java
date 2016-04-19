package org.shisoft.neb.durability;

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
        while (cacheQueue.size() > maxQueueItems) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        cacheQueue.put(new TrunkSegmentIdentifier(msgId, sid, trunkId, segId), data);
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
