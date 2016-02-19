package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashIntObjMap;
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;
import net.openhft.koloboke.collect.map.hash.HashObjIntMap;
import net.openhft.koloboke.collect.map.hash.HashObjIntMaps;

/**
 * Created by shisoft on 19/1/2016.
 */
public class schemaStore {

    HashIntObjMap schemaIdMap = HashIntObjMaps.newMutableMap();
    HashObjIntMap snameIdMap = HashObjIntMaps.newMutableMap();

    public schemaStore() {
    }

    public HashIntObjMap getSchemaIdMap() {
        return schemaIdMap;
    }

    public void setSchemaIdMap(HashIntObjMap schemaIdMap) {
        this.schemaIdMap = schemaIdMap;
    }

    public int put(int id, Object sname, Object schema){
        this.schemaIdMap.put(id, schema);
        this.snameIdMap.put(sname, id);
        return  id;
    }

    public void clear (){
        this.schemaIdMap.clear();
        this.snameIdMap.clear();
    }

    public int sname2Id(Object sname){
        return snameIdMap.getInt(sname);
    }

    public  void remove (int id, Object kw){
        schemaIdMap.remove(id);
        snameIdMap.remove(kw, id);
    }

}
