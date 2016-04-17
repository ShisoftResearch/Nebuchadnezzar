package org.shisoft.neb.io;

import org.shisoft.neb.Trunk;

import java.util.Date;
import java.util.UUID;

/**
 * Created by shisoft on 19/1/2016.
 */
public class Reader {

    public static char readChar(long offset) {
        return Trunk.getUnsafe().getChar(offset);
    }

    public static int readInt(long offset)  {
        return Trunk.getUnsafe().getInt(offset);
    }

    public static int readUnsignedShort(long offset) {
        short s = readShort(offset);
        return s < 0 ? (-1 * s) + Short.MAX_VALUE : s;
    }

    public static long readLong(long offset) {
        return Trunk.getUnsafe().getLong(offset);
    }

    public static boolean readBoolean(long offset) {
        int ch = readByte(offset);
        return (ch != 0);
    }

    public static short readShort(long offset) {
        return Trunk.getUnsafe().getShort(offset);
    }

    public static int readUshort(long offset) {
        return readUnsignedShort(offset);
    }

    public static byte readByte(long offset) {
        return Trunk.getUnsafe().getByte(offset);
    }

    public static byte[] readBytes(long offset, int len) {
        byte[] r = new byte[len];
        for (int i = 0; i < len; i++){
            r[i] = readByte(offset + i);
        }
        return r;
    }

    public static byte[] readBytes(long offset) {
        int len = readInt(offset);
        return readBytes(offset + type_lengths.intLen, len);
    }

    public static float readFloat(long offset) {
        return Trunk.getUnsafe().getFloat(offset);
    }

    public static double readDouble(long offset) {
        return Trunk.getUnsafe().getDouble(offset);
    }

    public static UUID readUuid(long offset) {
        long mb = readLong(offset);
        long lb = readLong(offset + type_lengths.longLen);
        return new UUID(mb, lb);
    }

    public static UUID readCid(long offset) {
        return readUuid(offset);
    }

    public static double[] readPos2d(long offset) {
        double x = readDouble(offset);
        offset += type_lengths.doubleLen;
        double y = readDouble(offset);
        return new double [] {x, y};
    }

    public static double[] readPos3d(long offset) {
        double x = readDouble(offset);
        offset += 8;
        double y = readDouble(offset);
        offset += 8;
        double z = readDouble(offset);
        return new double [] {x, y, z};
    }

    public static double[] readPos4d(long offset) {
        double x = readDouble(offset);
        offset += 8;
        double y = readDouble(offset);
        offset += 8;
        double z = readDouble(offset);
        offset += 8;
        double t = readDouble(offset);
        return new double [] {x, y, z, t};
    }

    public static float[] readGeo(long offset) {
        float lat = readFloat(offset);
        offset += 4;
        float lon = readFloat(offset);
        return new float [] {lat, lon};
    }

    public static Date readDate(long offset) {
        long timespan = readLong(offset);
        return new Date(timespan);
    }

}
