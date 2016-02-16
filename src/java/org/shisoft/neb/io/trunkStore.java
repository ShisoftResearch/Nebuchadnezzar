package org.shisoft.neb.io;

import org.shisoft.neb.trunk;

/**
 * Created by shisoft on 14/2/2016.
 */
public class trunkStore {

    trunk[] trunks;

    public void init(int trunkCount, long trunkSize) {
        trunks = new trunk[trunkCount];
        for (int i = 0; i < trunkCount; i++){
            trunks[i] = new trunk(trunkSize);
        }
    }

    public void dispose (){
        for (int i = 0; i < getTrunkCount(); i++){
            trunks[i].dispose();
            trunks[i] = null;
        }
    }

    public trunk getTrunk (int id){
        return trunks[id];
    }

    public int getTrunkCount(){
        return trunks.length;
    }

    public long getTrunkSize(){
        return trunks[0].getSize();
    }
}
