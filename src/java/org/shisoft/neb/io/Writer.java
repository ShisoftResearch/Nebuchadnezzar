package org.shisoft.neb.io;

import org.shisoft.neb.Trunk;

import java.util.Date;
import java.util.UUID;

/**
 * Created by shisoft on 19/1/2016.
 */
public class Writer {

    private static void setByte(Trunk trunk, long offset, int val){
        Trunk.getUnsafe().putByte(trunk.getStoreAddress() + offset, (byte) val);
    }

    public static void writeBytes(Trunk store, byte[] val, long offset){
        int length = val.length;
        writeInt(store, length, offset);
        offset += type_lengths.intLen;
        for (int i = 0 ; i < val.length ; i++){
            setByte(store, offset + i, val[i]);
        }
    }

    public static void writeChar(Trunk store, char v, long offset){
        Trunk.getUnsafe().putChar(store.getStoreAddress() + offset, v);
    }

    public static void writeInt(Trunk store, int val, long offset){
        Trunk.getUnsafe().putInt(store.getStoreAddress() + offset, val);
    }

    public static void writeShorts(Trunk store, short val, long offset){
        Trunk.getUnsafe().putShort(store.getStoreAddress() + offset, val);
    }

    public static void writeUshort(Trunk store, int v, long offset){
        writeShorts(store, (short)(v > Short.MAX_VALUE ? (-1 * (v - Short.MAX_VALUE)) : v), offset);
    }
    public static void writeShort(Trunk store, short v, long offset){
        writeShorts(store, v, offset);
    }

    public static void writeLong(Trunk store, long val, long offset){
        Trunk.getUnsafe().putLong(store.getStoreAddress() + offset, val);
    }

    public static void writeBoolean(Trunk store, boolean v, long offset){
        setByte(store, offset, (v ? 1 : 0));
    }

    public static void writeByte(Trunk store, byte v, long offset){
        setByte(store, offset, v);
    }

    public static void writeFloat(Trunk store, float v, long offset){
        Trunk.getUnsafe().putFloat(store.getStoreAddress() + offset, v);
    }

    public static void writeDouble(Trunk store, double v, long offset){
        Trunk.getUnsafe().putDouble(store.getStoreAddress() + offset, v);
    }

    public static void writeUuid(Trunk store, UUID v, long offset){
        long mb = v.getMostSignificantBits();
        long lb = v.getLeastSignificantBits();
        writeLong(store, mb, offset);
        writeLong(store, lb, offset + type_lengths.longLen);
    }

    public static void writeCid(Trunk store, UUID v, long offset){
        writeUuid(store, v, offset);
    }

    public static void writePos2d(Trunk store, double[] v, long offset){
        writeDouble(store, v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[1], offset);
    }

    public static void writePos3d(Trunk store, double[] v, long offset){
        writeDouble(store, v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[1], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[2], offset);
    }

    public static void writePos4d(Trunk store, double[] v, long offset){
        writeDouble(store, v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[1], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[2], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[3], offset);
    }

    public static void writeGeo(Trunk store, float[] v, long offset){
        writeFloat(store, v[0], offset);
        offset += type_lengths.floatLen;
        writeFloat(store, v[1], offset);
    }

    public static void writeDate(Trunk store, Date v, long offset){
        long timespan = v.getTime();
        writeLong(store, timespan, offset);
    }

}
