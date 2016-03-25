package org.shisoft.neb;

/**
 * Created by shisoft on 18/1/2016.
 */
public class Store {

    public long memSize = 0;
    public int trunkSize = 0;
    public Trunk[] trunks;

    public Store(long memSize, int trunkSize) {
        int trunkCount = ((Double) Math.floor(memSize / trunkSize)).intValue();
        this.trunkSize = trunkSize;
        trunks = new Trunk[trunkCount];
        for (int i = 0; i < trunkCount; i++){
            trunks[i] = new Trunk(trunkSize);
        }
        this.memSize = memSize;
    }
}
