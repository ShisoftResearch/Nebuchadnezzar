package org.shisoft.neb;

import org.shisoft.neb.io.type_lengths;

import java.io.EOFException;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;

/**
 * Created by shisoft on 18/1/2016.
 */
public class trunk {

    byte[] store;
    int committed = 0;
    public trunk(int size){
        store = new byte[size];
    }
    public byte[] getStore() {
        return store;
    }

    public  char readChar(int offset) throws EOFException {
        return (char)((store[offset] << 8) + (store[offset + 1] << 0));
    }

    public int readInt(int offset)  throws IOException {
        return ((store[offset] << 24) + (store[offset + 1] << 16) + (store[offset + 2] << 8) + (store[offset + 3] << 0));
    }

    public int readUnsignedShort(int offset) throws IOException {
        return (store[offset] << 8) + (store[offset + 1] << 0);
    }

    public String readText (int offset) throws  IOException { //TODO: Optimize for performance
        int length = readInt(offset);
        offset += type_lengths.intLen;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i< length; i++){
            sb.append(readChar(offset));
            offset += 2;
        }
        return sb.toString();
    }

    public long readLong(int offset) throws IOException {
        return (((long)store[offset] << 56) +
                ((store[offset + 1] & 255) << 48) +
                ((store[offset + 2] & 255) << 40) +
                ((store[offset + 3] & 255) << 32) +
                ((store[offset + 4] & 255) << 24) +
                ((store[offset + 5] & 255) << 16) +
                ((store[offset + 6] & 255) <<  8) +
                ((store[offset + 7] & 255) <<  0));
    }

    public boolean readBoolean (int offset) throws IOException{
        int ch = store[offset];
        if (ch < 0)
            throw new EOFException();
        return (ch != 0);
    }

    public short readShort(int offset) throws  IOException{
        return (short)((store[offset] << 8) + (store[offset + 1] << 0));
    }

    public int readUshort(int offset) throws  IOException{
        return readUnsignedShort(offset);
    }

    public byte readByte(int offset) throws IOException{
        return store[offset];
    }

    public float readFloat(int offset) throws  IOException {
        return Float.intBitsToFloat(readInt(offset));
    }

    public double readDouble (int offset) throws  IOException {
        return Double.longBitsToDouble(readLong(offset));
    }

    public UUID readUuid (int offset) throws  IOException {
        long mb = readLong(offset);
        long lb = readLong(offset + type_lengths.longLen);
        return new UUID(mb, lb);
    }

    public UUID readCid (int offset) throws  IOException {
        return readUuid(offset);
    }

    public double[] readPos2d(int offset) throws  IOException {
        double x = readDouble(offset);
        offset += type_lengths.doubleLen;
        double y = readDouble(offset);
        return new double [] {x, y};
    }

    public double[] readPos3d(int offset) throws  IOException {
        double x = readDouble(offset);
        offset += 8;
        double y = readDouble(offset);
        offset += 8;
        double z = readDouble(offset);
        return new double [] {x, y, z};
    }

    public double[] readPos4d(int offset) throws  IOException {
        double x = readDouble(offset);
        offset += 8;
        double y = readDouble(offset);
        offset += 8;
        double z = readDouble(offset);
        offset += 8;
        double t = readDouble(offset);
        return new double [] {x, y, z, t};
    }

    public float[] readGeo(int offset) throws  IOException {
        float lat = readFloat(offset);
        offset += 4;
        float lon = readFloat(offset);
        return new float [] {lat, lon};
    }

    public Date readDate(int offset) throws  IOException {
        long timespan = readLong(offset);
        return new Date(timespan);
    }

    ///////////////////////////////////////////////////////////////

    private void put(int offset, int val){
        store[offset] = (byte) val;
    }

    public void writeChar(char v, int offset){
        put(offset, (v >>> 8) & 0xFF);
        put(offset + 1, (v >>> 0) & 0xFF);
    }

    public void writeInt(int v, int offset){
        put(offset, (v >>> 24) & 0xFF);
        put(offset + 1, (v >>> 16) & 0xFF);
        put(offset + 2, (v >>>  8) & 0xFF);
        put(offset + 3, (v >>> 0) & 0xFF);
    }

    public void writeShorts(int v, int offset){
        put(offset, (v >>> 8) & 0xFF);
        put(offset + 1, (v >>> 0) & 0xFF);
    }

    public void writeUshort(int v, int offset){
        writeShorts(v, offset);
    }

    public void writeShort(short v, int offset){
        writeShorts(v, offset);
    }

    public void writeText(String v, int offset){ //TODO: Optimize for performance
        int length = v.length();
        writeInt(length, offset);
        offset += type_lengths.intLen;
        for (char c : v.toCharArray()){
            writeChar(c, offset);
            offset += type_lengths.charLen;
        }
    }

    public void writeLong(long v, int offset){
        put(offset, (int) (v >>> 56));
        put(offset + 1, (int) (v >>> 48));
        put(offset + 2, (int) (v >>> 40));
        put(offset + 3, (int) (v >>> 32));
        put(offset + 4, (int) (v >>> 24));
        put(offset + 5, (int) (v >>> 16));
        put(offset + 6, (int) (v >>> 8));
        put(offset + 7, (int) (v >>> 0));
    }

    public void writeBoolean (boolean v, int offset){
        put(offset, (v ? 1 : 0));
    }

    public void writeByte (byte v, int offset){
        put(offset, v);
    }

    public void writeFloat(float v, int offset){
        writeInt(Float.floatToIntBits(v), offset);
    }

    public void writeDouble (double v, int offset){
        writeLong(Double.doubleToLongBits(v), offset);
    }

    public void writeUuid (UUID v, int offset){
        long mb = v.getMostSignificantBits();
        long lb = v.getLeastSignificantBits();
        writeLong(mb, offset);
        writeLong(lb, offset + type_lengths.longLen);
    }

    public void writeCid(UUID v, int offset){
        writeUuid(v, offset);
    }

    public void writePos2d(double[] v, int offset){
        writeDouble(v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(v[1], offset);
    }

    public void writePos3d(double[] v, int offset){
        writeDouble(v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(v[1], offset);
        offset += type_lengths.doubleLen;
        writeDouble(v[2], offset);
    }

    public void writePos4d(double[] v, int offset){
        writeDouble(v[0], offset);
        offset += type_lengths.doubleLen;
        writeDouble(v[1], offset);
        offset += type_lengths.doubleLen;
        writeDouble(v[2], offset);
        offset += type_lengths.doubleLen;
        writeDouble(v[3], offset);
    }

    public void writeGeo(float[] v, int offset){
        writeFloat(v[0], offset);
        offset += type_lengths.doubleLen;
        writeFloat(v[1], offset);
    }

    public void writeDate(Date v, int offset){
        long timespan = v.getTime();
        writeLong(timespan, offset);
    }

}
