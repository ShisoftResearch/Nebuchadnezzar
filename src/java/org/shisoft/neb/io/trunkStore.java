package org.shisoft.neb.io;

import org.shisoft.neb.trunk;

/**
 * Created by shisoft on 14/2/2016.
 */
public class trunkStore {

    trunk[] trunks;

    public void init(int trunkCount, long trunkSize) {
        trunks = new trunk[trunkCount - 1];
        for (int i = 0; i < trunkCount; i++){
            trunks[i] = new trunk(trunkSize);
        }
    }

    public trunk getTrunk (int id){
        return trunks[id];
    }

    public int getTrunkCount(){
        return trunks.length;
    }
}
