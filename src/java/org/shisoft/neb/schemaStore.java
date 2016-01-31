package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashShortObjMap;
import net.openhft.koloboke.collect.map.hash.HashShortObjMaps;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by shisoft on 19/1/2016.
 */
public class schemaStore {

    AtomicInteger idCounter = new AtomicInteger(0);
    HashShortObjMap schemaIdMap = HashShortObjMaps.newMutableMap();

    public schemaStore() {
    }

    public AtomicInteger getIdCounter() {
        return idCounter;
    }

    public void setIdCounter(AtomicInteger idCounter) {
        this.idCounter = idCounter;
    }

    public HashShortObjMap getSchemaIdMap() {
        return schemaIdMap;
    }

    public void setSchemaIdMap(HashShortObjMap schemaIdMap) {
        this.schemaIdMap = schemaIdMap;
    }
}
