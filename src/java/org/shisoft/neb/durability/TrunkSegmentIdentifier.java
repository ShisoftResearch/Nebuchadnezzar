package org.shisoft.neb.durability;

/**
 * Created by shisoft on 16-4-18.
 */
public class TrunkSegmentIdentifier {

    private long msgId;
    private int sid;
    private int trunkId;
    private int segId;

    public TrunkSegmentIdentifier(long msgId, int sid, int trunkId, int segId) {
        this.msgId = msgId;
        this.sid = sid;
        this.trunkId = trunkId;
        this.segId = segId;
    }

    public long getMsgId() {
        return msgId;
    }

    public void setMsgId(long msgId) {
        this.msgId = msgId;
    }

    public int getSid() {
        return sid;
    }

    public void setSid(int sid) {
        this.sid = sid;
    }

    public int getTrunkId() {
        return trunkId;
    }

    public void setTrunkId(int trunkId) {
        this.trunkId = trunkId;
    }

    public int getSegId() {
        return segId;
    }

    public void setSegId(int segId) {
        this.segId = segId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TrunkSegmentIdentifier that = (TrunkSegmentIdentifier) o;

        if (sid != that.sid) return false;
        if (trunkId != that.trunkId) return false;
        return segId == that.segId;

    }

    @Override
    public int hashCode() {
        int result = sid;
        result = 31 * result + trunkId;
        result = 31 * result + segId;
        return result;
    }
}
