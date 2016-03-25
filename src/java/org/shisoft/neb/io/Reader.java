package org.shisoft.neb.io;

import org.shisoft.neb.Trunk;

import java.util.Date;
import java.util.UUID;

/**
 * Created by shisoft on 19/1/2016.
 */
public class Reader {

    public static char readChar(Trunk store, long offset) {
        return Trunk.getUnsafe().getChar(store.getStoreAddress() + offset);
    }

    public static int readInt(Trunk store, long offset)  {
        return Trunk.getUnsafe().getInt(store.getStoreAddress() + offset);
    }

    public static int readUnsignedShort(Trunk store, long offset) {
        short s = readShort(store, offset);
        return s < 0 ? (-1 * s) + Short.MAX_VALUE : s;
    }

    public static long readLong(Trunk store, long offset) {
        return Trunk.getUnsafe().getLong(store.getStoreAddress() + offset);
    }

    public static boolean readBoolean(Trunk store, long offset) {
        int ch = readByte(store, offset);
        return (ch != 0);
    }

    public static short readShort(Trunk store, long offset) {
        return Trunk.getUnsafe().getShort(store.getStoreAddress() + offset);
    }

    public static int readUshort(Trunk store, long offset) {
        return readUnsignedShort(store, offset);
    }

    public static byte readByte(Trunk store, long offset) {
        return Trunk.getUnsafe().getByte(store.getStoreAddress() + offset);
    }

    public static byte[] readBytes(Trunk store, long offset, int len) {
        byte[] r = new byte[len];
        for (int i = 0; i < len; i++){
            r[i] = readByte(store, offset + i);
        }
        return r;
    }

    public static byte[] readBytes(Trunk store, long offset) {
        int len = readInt(store, offset);
        return readBytes(store, offset + type_lengths.intLen, len);
    }

    public static float readFloat(Trunk store, long offset) {
        return Trunk.getUnsafe().getFloat(store.getStoreAddress() + offset);
    }

    public static double readDouble(Trunk store, long offset) {
        return Trunk.getUnsafe().getDouble(store.getStoreAddress() + offset);
    }

    public static UUID readUuid(Trunk store, long offset) {
        long mb = readLong(store, offset);
        long lb = readLong(store, offset + type_lengths.longLen);
        return new UUID(mb, lb);
    }

    public static UUID readCid(Trunk store, long offset) {
        return readUuid(store, offset);
    }

    public static double[] readPos2d(Trunk store, long offset) {
        double x = readDouble(store, offset);
        offset += type_lengths.doubleLen;
        double y = readDouble(store, offset);
        return new double [] {x, y};
    }

    public static double[] readPos3d(Trunk store, long offset) {
        double x = readDouble(store, offset);
        offset += 8;
        double y = readDouble(store, offset);
        offset += 8;
        double z = readDouble(store, offset);
        return new double [] {x, y, z};
    }

    public static double[] readPos4d(Trunk store, long offset) {
        double x = readDouble(store, offset);
        offset += 8;
        double y = readDouble(store, offset);
        offset += 8;
        double z = readDouble(store, offset);
        offset += 8;
        double t = readDouble(store, offset);
        return new double [] {x, y, z, t};
    }

    public static float[] readGeo(Trunk store, long offset) {
        float lat = readFloat(store, offset);
        offset += 4;
        float lon = readFloat(store, offset);
        return new float [] {lat, lon};
    }

    public static Date readDate(Trunk store, long offset) {
        long timespan = readLong(store, offset);
        return new Date(timespan);
    }

}
