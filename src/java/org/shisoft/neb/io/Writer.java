package org.shisoft.neb.io;

import org.shisoft.neb.Trunk;

import java.util.Date;
import java.util.UUID;

/**
 * Created by shisoft on 19/1/2016.
 */
public class Writer {

    private static void setByte(long offset, int val){
        Trunk.getUnsafe().putByte(offset, (byte) val);
    }

    public static void writeRawBytes(byte[] val, long offset){
        for (int i = 0 ; i < val.length ; i++){
            setByte(offset + i, val[i]);
        }
    }

    public static void writeBytes(byte[] val, long offset){
        int length = val.length;
        writeInt(length, offset);
        offset += type_lengths.intLen;
        writeRawBytes(val, offset);
    }

    public static void writeChar(char v, long offset){
        Trunk.getUnsafe().putChar(offset, v);
    }

    public static void writeInt(int val, long offset){
        Trunk.getUnsafe().putInt(offset, val);
    }

    public static void writeShorts(short val, long offset){
        Trunk.getUnsafe().putShort(offset, val);
    }

    public static void writeUshort(int v, long offset){
        writeShorts((short)(v > Short.MAX_VALUE ? (-1 * (v - Short.MAX_VALUE)) : v), offset);
    }
    public static void writeShort(short v, long offset){
        writeShorts(v, offset);
    }

    public static void writeLong(long val, long offset){
        Trunk.getUnsafe().putLong(offset, val);
    }

    public static void writeBoolean(boolean v, long offset){
        setByte(offset, (v ? 1 : 0));
    }

    public static void writeByte(byte v, long offset){
        setByte(offset, v);
    }

    public static void writeFloat(float v, long offset){
        Trunk.getUnsafe().putFloat(offset, v);
    }

    public static void writeDouble(double v, long offset){
        Trunk.getUnsafe().putDouble(offset, v);
    }

    public static void writeUuid(UUID v, long offset){
        long mb = v.getMostSignificantBits();
        long lb = v.getLeastSignificantBits();
        writeLong(mb, offset);
        writeLong(lb, offset + type_lengths.longLen);
    }

    public static void writeCid(UUID v, long offset){
        writeUuid(v, offset);
    }

    public static void writePos2d(double[] v, long offset){
        writeDouble(v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(v[1], offset);
    }

    public static void writePos3d(double[] v, long offset){
        writeDouble(v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(v[1], offset);
        offset += type_lengths.doubleLen;
        writeDouble(v[2], offset);
    }

    public static void writePos4d(double[] v, long offset){
        writeDouble(v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(v[1], offset);
        offset += type_lengths.doubleLen;
        writeDouble(v[2], offset);
        offset += type_lengths.doubleLen;
        writeDouble(v[3], offset);
    }

    public static void writeGeo(float[] v, long offset){
        writeFloat(v[0], offset);
        offset += type_lengths.floatLen;
        writeFloat(v[1], offset);
    }

    public static void writeDate(Date v, long offset){
        long timespan = v.getTime();
        writeLong(timespan, offset);
    }

}
