package org.shisoft.neb.io;

import org.shisoft.neb.trunk;

import java.util.Date;
import java.util.UUID;

/**
 * Created by shisoft on 19/1/2016.
 */
public class writer {

    private static void setByte(trunk trunk, long offset, int val){
        org.shisoft.neb.trunk.getUnsafe().putByte(trunk.getStoreAddress() + offset, (byte) val);
    }

    public static void writeBytes(trunk store, byte[] val, long offset){
        int length = val.length;
        writeInt(store, length, offset);
        offset += type_lengths.intLen;
        for (int i = 0 ; i < val.length ; i++){
            setByte(store, offset + i, val[i]);
        }
    }

    public static void writeChar(trunk store, char v, long offset){
        trunk.getUnsafe().putChar(store.getStoreAddress() + offset, v);
    }

    public static void writeInt(trunk store, int val, long offset){
        trunk.getUnsafe().putInt(store.getStoreAddress() + offset, val);
    }

    public static void writeShorts(trunk store, short val, long offset){
        trunk.getUnsafe().putShort(store.getStoreAddress() + offset, val);
    }

    public static void writeUshort(trunk store, int v, long offset){
        writeShorts(store, (short)(v > Short.MAX_VALUE ? (-1 * (v - Short.MAX_VALUE)) : v), offset);
    }
    public static void writeShort(trunk store, short v, long offset){
        writeShorts(store, v, offset);
    }

    public static void writeLong(trunk store, long val, long offset){
        trunk.getUnsafe().putLong(store.getStoreAddress() + offset, val);
    }

    public static void writeBoolean(trunk store, boolean v, long offset){
        setByte(store, offset, (v ? 1 : 0));
    }

    public static void writeByte(trunk store, byte v, long offset){
        setByte(store, offset, v);
    }

    public static void writeFloat(trunk store, float v, long offset){
        trunk.getUnsafe().putFloat(store.getStoreAddress() + offset, v);
    }

    public static void writeDouble(trunk store, double v, long offset){
        trunk.getUnsafe().putDouble(store.getStoreAddress() + offset, v);
    }

    public static void writeUuid(trunk store, UUID v, long offset){
        long mb = v.getMostSignificantBits();
        long lb = v.getLeastSignificantBits();
        writeLong(store, mb, offset);
        writeLong(store, lb, offset + type_lengths.longLen);
    }

    public static void writeCid(trunk store, UUID v, long offset){
        writeUuid(store, v, offset);
    }

    public static void writePos2d(trunk store, double[] v, long offset){
        writeDouble(store, v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[1], offset);
    }

    public static void writePos3d(trunk store, double[] v, long offset){
        writeDouble(store, v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[1], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[2], offset);
    }

    public static void writePos4d(trunk store, double[] v, long offset){
        writeDouble(store, v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[1], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[2], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[3], offset);
    }

    public static void writeGeo(trunk store, float[] v, long offset){
        writeFloat(store, v[0], offset);
        offset += type_lengths.floatLen;
        writeFloat(store, v[1], offset);
    }

    public static void writeDate(trunk store, Date v, long offset){
        long timespan = v.getTime();
        writeLong(store, timespan, offset);
    }

}
