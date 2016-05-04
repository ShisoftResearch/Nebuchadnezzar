package org.shisoft.neb;

import net.openhft.koloboke.collect.map.hash.HashIntObjMap;
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;
import net.openhft.koloboke.collect.map.hash.HashObjIntMap;
import net.openhft.koloboke.collect.map.hash.HashObjIntMaps;

/**
 * Created by shisoft on 19/1/2016.
 */
public class SchemaStore {

    HashIntObjMap schemaIdMap = HashIntObjMaps.newMutableMap();
    HashObjIntMap snameIdMap = HashObjIntMaps.newMutableMap();

    public SchemaStore() {
    }

    public HashIntObjMap getSchemaIdMap() {
        return schemaIdMap;
    }

    public synchronized int put(int id, Object sname, Object schema){
        this.schemaIdMap.put(id, schema);
        this.snameIdMap.put(sname, id);
        return  id;
    }

    public synchronized Object getById (int id){
        return schemaIdMap.get(id);
    }

    public synchronized boolean snameExists (Object sname){
        return this.snameIdMap.containsKey(sname);
    }

    public synchronized void clear (){
        this.schemaIdMap.clear();
        this.snameIdMap.clear();
    }

    public synchronized int sname2Id(Object sname){
        return snameIdMap.getInt(sname);
    }

    public synchronized void remove (int id, Object kw){
        schemaIdMap.remove(id);
        snameIdMap.remove(kw, id);
    }

    @Override
    public String toString() {
        return "SchemaStore{" +
                "schemaIdMap=" + schemaIdMap +
                '}';
    }
}
