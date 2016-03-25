package org.shisoft.neb.durability;

import org.shisoft.neb.Trunk;
import org.shisoft.neb.durability.io.BufferedRandomAccessFile;

import java.io.IOException;

/**
 * Created by shisoft on 16-3-25.
 */
public class BackStore {
    Trunk trunk;
    BufferedRandomAccessFile memoryBRAF;
    String basePath;
    String metaPath;
    public BufferedRandomAccessFile getMemoryBRAF() {
        return memoryBRAF;
    }

    public BackStore(String basePath) throws IOException {
        this.basePath = basePath;
        setMemoryBackend(basePath + ".dat");
        setMetaBackend(basePath + ".meta");
    }

    private void setMemoryBackend(String path) throws IOException {
        BufferedRandomAccessFile braf = new BufferedRandomAccessFile(path, "rw");
        if (braf.length() > 0){
            for (int i = 0; i < braf.length(); i++){
                braf.seek(i);
                trunk.getUnsafe().putByte(trunk.getStoreAddress() + i, braf.readByte());
            }
        }
        this.memoryBRAF = braf;
    }
    private void setMetaBackend(String path) throws IOException {
        metaPath = path;
    }
    public void close () throws IOException {
        if (memoryBRAF != null) memoryBRAF.close();
    }
    public void syncRange (long start, long end) throws IOException {
        memoryBRAF.seek(start);
        for (long i = start; i <= end; i++){
            memoryBRAF.write(trunk.getMemoryFork().getByte(trunk.getStoreAddress() + i));
        }
    }
    public void resetTail (long pos) throws IOException {
        memoryBRAF.getChannel().truncate(pos);
    }
    public String getMetaPath() {
        return metaPath;
    }
}
