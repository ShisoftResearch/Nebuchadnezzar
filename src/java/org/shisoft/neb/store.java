package org.shisoft.neb;

/**
 * Created by shisoft on 18/1/2016.
 */
public class store {

    public long memSize = 0;
    public int trunkSize = 0;
    public trunk[] trunks;

    public store(long memSize, int trunkSize) {
        int trunkCount = ((Double) Math.floor(memSize / trunkSize)).intValue();
        this.trunkSize = trunkSize;
        trunks = new trunk[trunkCount];
        for (int i = 0; i < trunkCount; i++){
            trunks[i] = new trunk(trunkSize);
        }
        this.memSize = memSize;
    }
}
