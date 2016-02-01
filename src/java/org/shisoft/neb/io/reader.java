package org.shisoft.neb.io;

import org.shisoft.neb.trunk;

import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

/**
 * Created by shisoft on 19/1/2016.
 */
public class reader {

    public static char readChar(trunk trunk, int offset) {
        return (char)readShort(trunk, offset);
    }

    public static int readInt(trunk trunk, int offset)  {
        byte[] store = trunk.getStore();
        int n = 0;
        for(int i = offset; i < (offset + type_lengths.intLen); i++) {
            n <<= 8;
            n ^= store[i] & 0xFF;
        }
        return n;
    }

    public static int readUnsignedShort(trunk trunk, int offset) {
        byte[] store = trunk.getStore();
        return (store[offset] << 8) + (store[offset + 1] << 0);
    }

    public static long readLong(trunk trunk, int offset) {
        byte[] store = trunk.getStore();
        long l = 0;
        for(int i = offset; i < offset + type_lengths.longLen; i++) {
            l <<= 8;
            l ^= store[i] & 0xFF;
        }
        return l;
    }

    public static boolean readBoolean(trunk trunk, int offset) {
        byte[] store = trunk.getStore();
        int ch = store[offset];
        return (ch != 0);
    }

    public static short readShort(trunk trunk, int offset) {
        byte[] store = trunk.getStore();
        return (short)((store[offset] << 8) + (store[offset + 1] << 0));
    }

    public static int readUshort(trunk trunk, int offset) {
        return readUnsignedShort(trunk, offset);
    }

    public static byte readByte(trunk trunk, int offset) {
        byte[] store = trunk.getStore();
        return store[offset];
    }

    public static byte[] readBytes(trunk trunk, int offset, int len) {
        byte[] store = trunk.getStore();
        return Arrays.copyOfRange(store, offset, offset + len);
    }

    public static byte[] readBytes(trunk trunk, int offset) {
        int len = readInt(trunk, offset);
        return readBytes(trunk, offset + type_lengths.intLen, len);
    }

    public static float readFloat(trunk trunk, int offset) {
        return Float.intBitsToFloat(readInt(trunk, offset));
    }

    public static double readDouble(trunk trunk, int offset) {
        return Double.longBitsToDouble(readLong(trunk, offset));
    }

    public static UUID readUuid(trunk trunk, int offset) {
        long mb = readLong(trunk, offset);
        long lb = readLong(trunk, offset + type_lengths.longLen);
        return new UUID(mb, lb);
    }

    public static UUID readCid(trunk trunk, int offset) {
        return readUuid(trunk, offset);
    }

    public static double[] readPos2d(trunk trunk, int offset) {
        double x = readDouble(trunk, offset);
        offset += type_lengths.doubleLen;
        double y = readDouble(trunk, offset);
        return new double [] {x, y};
    }

    public static double[] readPos3d(trunk trunk, int offset) {
        double x = readDouble(trunk, offset);
        offset += 8;
        double y = readDouble(trunk, offset);
        offset += 8;
        double z = readDouble(trunk, offset);
        return new double [] {x, y, z};
    }

    public static double[] readPos4d(trunk trunk, int offset) {
        double x = readDouble(trunk, offset);
        offset += 8;
        double y = readDouble(trunk, offset);
        offset += 8;
        double z = readDouble(trunk, offset);
        offset += 8;
        double t = readDouble(trunk, offset);
        return new double [] {x, y, z, t};
    }

    public static float[] readGeo(trunk trunk, int offset) {
        float lat = readFloat(trunk, offset);
        offset += 4;
        float lon = readFloat(trunk, offset);
        return new float [] {lat, lon};
    }

    public static Date readDate(trunk trunk, int offset) {
        long timespan = readLong(trunk, offset);
        return new Date(timespan);
    }

}
