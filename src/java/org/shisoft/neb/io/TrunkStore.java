package org.shisoft.neb.io;

import org.shisoft.neb.Trunk;

import java.io.IOException;

/**
 * Created by shisoft on 14/2/2016.
 */
public class TrunkStore {

    Trunk[] trunks;

    public void init(int trunkCount, long trunkSize, boolean durability) {
        trunks = new Trunk[trunkCount];
        for (int i = 0; i < trunkCount; i++){
            trunks[i] = new Trunk(trunkSize, i);
            if (durability){
                trunks[i].enableDurability();
            }
        }
    }

    public void dispose () throws IOException {
        for (int i = 0; i < getTrunkCount(); i++){
            trunks[i].dispose();
            trunks[i] = null;
        }
    }

    public Trunk[] getTrunks() {
        return trunks;
    }

    public Trunk getTrunk (int id){
        return trunks[id];
    }

    public int getTrunkCount(){
        return trunks.length;
    }

    public long getTrunkSize(){
        return trunks[0].getSize();
    }

    public long[] getTrunksCellCount (){
        long[] r = new long[getTrunkCount()];
        for (int i = 0; i < getTrunkCount(); i++){
            r[i] = trunks[i].getCellIndex().size();
        }
        return r;
    }
}
