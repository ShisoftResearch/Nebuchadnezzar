package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashIntObjMap;
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;

/**
 * Created by shisoft on 19/1/2016.
 */
public class schemaStore {

    HashIntObjMap schemaIdMap = HashIntObjMaps.newMutableMap();

    public schemaStore() {
    }

    public HashIntObjMap getSchemaIdMap() {
        return schemaIdMap;
    }

    public void setSchemaIdMap(HashIntObjMap schemaIdMap) {
        this.schemaIdMap = schemaIdMap;
    }

    public int put(int id, Object schema){
        this.schemaIdMap.put(id, schema);
        return  id;
    }

}
