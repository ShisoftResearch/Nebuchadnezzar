package org.shisoft.neb.io;

import org.shisoft.neb.trunk;

import java.util.Date;
import java.util.UUID;

/**
 * Created by shisoft on 19/1/2016.
 */
public class writer {

    private static void setByte(trunk trunk, int offset, int val){
        trunk.getStore()[offset] = (byte) val;
    }

    public static void writeBytes(trunk store, byte[] val, int offset){
        int length = val.length;
        writeInt(store, length, offset);
        offset += type_lengths.intLen;
        for (int i = 0 ; i < val.length ; i++){
            setByte(store, offset + i, val[i]);
        }
    }

    public static void writeChar(trunk store, char v, int offset){
        setByte(store, offset, (v >>> 8) & 0xFF);
        setByte(store, offset + 1, (v >>> 0) & 0xFF);
    }

    public static void writeChar(trunk store, char v){
        writeChar(store, v, store.getPointer().getAndAdd(type_lengths.charLen));
    }

    public static void writeInt(trunk store, int val, int offset){
        for(int i= offset + 3; i > offset; i--) {
            setByte(store, i, (byte) val);
            val >>>= 8;
        }
        setByte(store, offset, (byte) val);
    }

    public static void writeInt(trunk store, int v){
        writeInt(store, v, store.getPointer().getAndAdd(type_lengths.intLen));
    }

    public static void writeShorts(trunk store, int val, int offset){
        for(int i = offset + (type_lengths.shortLen - 1); i > offset; i--) {
            setByte(store, i, (byte) val);
            val >>>= 8;
        }
        setByte(store, offset, (byte) val);
    }

    public static void writeShorts(trunk store, int v){
        writeShorts(store, v, store.getPointer().getAndAdd(type_lengths.shortLen));
    }

    public static void writeUshort(trunk store, int v, int offset){
        writeShorts(store, (v > Short.MAX_VALUE ? (-1 * (v - Short.MAX_VALUE)) : v), offset);
    }

    public static void writeUshort(trunk store, int v){
        writeUshort(store, v, store.getPointer().getAndAdd(type_lengths.ushortLen));
    }

    public static void writeShort(trunk store, short v, int offset){
        writeShorts(store, v, offset);
    }

    public static void writeShort(trunk store, short v){
        writeShorts(store, v);
    }

    public static void writeLong(trunk store, long val, int offset){
        for(int i = offset + 7; i > offset; i--) {
            setByte(store, i, (byte) val);
            val >>>= 8;
        }
        setByte(store, offset, (byte) val);
    }

    public static void writeLong(trunk store, long v){
        writeLong(store, v, store.getPointer().getAndAdd(type_lengths.longLen));
    }

    public static void writeBoolean(trunk store, boolean v, int offset){
        setByte(store, offset, (v ? 1 : 0));
    }

    public static void writeBoolean(trunk store, boolean v){
        writeBoolean(store, v, store.getPointer().getAndAdd(type_lengths.booleanLen));
    }

    public static void writeByte(trunk store, byte v, int offset){
        setByte(store, offset, v);
    }

    public static void writeByte(trunk store, byte v){
        writeByte(store, v, store.getPointer().getAndAdd(type_lengths.byteLen));
    }

    public static void writeFloat(trunk store, float v, int offset){
        writeInt(store, Float.floatToIntBits(v), offset);
    }

    public static void writeFloat(trunk store, float v){
        writeFloat(store, v, store.getPointer().getAndAdd(type_lengths.floatLen));
    }

    public static void writeDouble(trunk store, double v, int offset){
        writeLong(store, Double.doubleToLongBits(v), offset);
    }

    public static void writeDouble(trunk store, double v){
        writeDouble(store, v, store.getPointer().getAndAdd(type_lengths.doubleLen));
    }

    public static void writeUuid(trunk store, UUID v, int offset){
        long mb = v.getMostSignificantBits();
        long lb = v.getLeastSignificantBits();
        writeLong(store, mb, offset);
        writeLong(store, lb, offset + type_lengths.longLen);
    }

    public static void writeUuid(trunk store, UUID v){
        writeUuid(store, v, store.getPointer().getAndAdd(type_lengths.uuidLen));
    }

    public static void writeCid(trunk store, UUID v, int offset){
        writeUuid(store, v, offset);
    }

    public static void writeCid(trunk store, UUID v){
        writeUuid(store, v);
    }

    public static void writePos2d(trunk store, double[] v, int offset){
        writeDouble(store, v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[1], offset);
    }

    public static void writePos2d(trunk store, double[] v){
        writePos2d(store, v, store.getPointer().getAndAdd(type_lengths.pos2dLen));
    }

    public static void writePos3d(trunk store, double[] v, int offset){
        writeDouble(store, v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[1], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[2], offset);
    }

    public static void writePos3d(trunk store, double[] v){
        writePos3d(store, v, store.getPointer().getAndAdd(type_lengths.pos3dLen));
    }

    public static void writePos4d(trunk store, double[] v, int offset){
        writeDouble(store, v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[1], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[2], offset);
        offset += type_lengths.doubleLen;
        writeDouble(store, v[3], offset);
    }

    public static void writePos4d(trunk store, double[] v){
        writePos4d(store, v, store.getPointer().getAndAdd(type_lengths.pos4dLen));
    }

    public static void writeGeo(trunk store, float[] v, int offset){
        writeFloat(store, v[0], offset);
        offset += type_lengths.floatLen;
        writeFloat(store, v[1], offset);
    }

    public static void writeGeo(trunk store, float[] v){
        writeGeo(store, v, store.getPointer().getAndAdd(type_lengths.geoLen));
    }

    public static void writeDate(trunk store, Date v, int offset){
        long timespan = v.getTime();
        writeLong(store, timespan, offset);
    }

    public static void writeDate(trunk store, Date v){
        writeDate(store, v, store.getPointer().getAndAdd(type_lengths.dateLen));
    }

}
