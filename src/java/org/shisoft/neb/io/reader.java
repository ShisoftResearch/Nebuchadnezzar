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
        byte[] store = trunk.getStore();
        return (char)((store[offset] << 8) + (store[offset + 1] << 0));
    }

    public static int readInt(trunk trunk, int offset)  {
        byte[] store = trunk.getStore();
        return ((store[offset] << 24) + (store[offset + 1] << 16) + (store[offset + 2] << 8) + (store[offset + 3] << 0));
    }

    public static int readUnsignedShort(trunk trunk, int offset) {
        byte[] store = trunk.getStore();
        return (store[offset] << 8) + (store[offset + 1] << 0);
    }

    public static String readText(trunk trunk, int offset) { //TODO: Optimize for performance
        int length = readInt(trunk, offset);
        offset += type_lengths.intLen;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i< length; i++){
            sb.append(readChar(trunk, offset));
            offset += 2;
        }
        return sb.toString();
    }

    public static long readLong(trunk trunk, int offset) {
        byte[] store = trunk.getStore();
        return (((long) store[offset] << 56) +
                ((store[offset + 1] & 255) << 48) +
                ((store[offset + 2] & 255) << 40) +
                ((store[offset + 3] & 255) << 32) +
                ((store[offset + 4] & 255) << 24) +
                ((store[offset + 5] & 255) << 16) +
                ((store[offset + 6] & 255) <<  8) +
                ((store[offset + 7] & 255) <<  0));
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
