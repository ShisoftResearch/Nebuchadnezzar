package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashIntObjMap;
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by shisoft on 19/1/2016.
 */
public class schemaStore {

    AtomicInteger idCounter = new AtomicInteger(0);
    HashIntObjMap schemaIdMap = HashIntObjMaps.newMutableMap();

    public schemaStore() {
    }

    public AtomicInteger getIdCounter() {
        return idCounter;
    }

    public void setIdCounter(AtomicInteger idCounter) {
        this.idCounter = idCounter;
    }

    public HashIntObjMap getSchemaIdMap() {
        return schemaIdMap;
    }

    public void setSchemaIdMap(HashIntObjMap schemaIdMap) {
        this.schemaIdMap = schemaIdMap;
    }
}
