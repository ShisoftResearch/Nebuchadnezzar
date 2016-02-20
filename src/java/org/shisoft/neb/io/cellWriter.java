package org.shisoft.neb.io;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

/**
 * Created by shisoft on 21/1/2016.
 */
public class cellWriter {

    static IFn defragFn = Clojure.var("neb.defragment", "scan-trunk-and-defragment");
    long startLoc;
    long currLoc;
    org.shisoft.neb.trunk trunk;

    private void init(org.shisoft.neb.trunk trunk, long length, long currLoc){
        this.trunk = trunk;
        this.currLoc = currLoc;
        this.startLoc = currLoc;
    }

    public cellWriter(org.shisoft.neb.trunk trunk, long length) throws Exception {
        tryAllocate(trunk, length, false);
    }

    public void tryAllocate(org.shisoft.neb.trunk trunk, long length, boolean defraged){
        trunk.getCellWriterLock().lock();
        try {
            long loc = trunk.getAppendHeader().getAndAdd(length);
            if (loc + length > trunk.getSize()){
                trunk.getAppendHeader().set(loc);
                if (defraged){
                    throw new Exception("Store full, expected length:" + length + " remains:" + (trunk.getSize() - loc));
                } else {
                    defragFn.invoke(trunk);
                    tryAllocate(trunk, length, true);
                }
            }  else {
                init(trunk, length, loc);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            trunk.getCellWriterLock().unlock();
        }
    }

    public cellWriter(org.shisoft.neb.trunk trunk, long length, long currLoc){
        init(trunk, length, currLoc);
    }

    public void streamWrite (IFn fn, Object value, long length){
        fn.invoke(trunk, value, currLoc);
        currLoc += length;
    }

    public void addCellToTrunkIndex(long hash){
        trunk.getCellIndex().put(hash, new cellMeta(startLoc));
    }

    public void updateCellToTrunkIndex(long hash){
        trunk.getCellIndex().get(hash).setLocation(startLoc);
    }

    public void lockIndex (){
        trunk.getIndexWriteLock().lock();
    }

    public void unlockIndex (){
        trunk.getIndexWriteLock().unlock();
    }

    public long getCurrLoc() {
        return currLoc;
    }
}
