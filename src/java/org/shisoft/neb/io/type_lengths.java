package org.shisoft.neb.io;

/**
 * Created by shisoft on 29/9/14.
 */
public class type_lengths {
    public static final short charLen = Character.SIZE / Byte.SIZE;
    public static final short intLen = Integer.SIZE / Byte.SIZE;
    public static final short longLen = Long.SIZE / Byte.SIZE;
    public static final short booleanLen = Byte.SIZE / Byte.SIZE;
    public static final short shortLen = Short.SIZE / Byte.SIZE;
    public static final short ushortLen = shortLen;
    public static final short byteLen = Byte.SIZE / Byte.SIZE;
    public static final short floatLen = Float.SIZE / Byte.SIZE;
    public static final short doubleLen = Double.SIZE / Byte.SIZE;
    public static final short uuidLen = 2 * longLen;
    public static final short cidLen = uuidLen;
    public static final short pos2dLen = 2 * doubleLen;
    public static final short pos3dLen = 3 * doubleLen;
    public static final short pos4dLen = 4 * doubleLen;
    public static final short geoLen = 2 * floatLen;
    public static final short dateLen = longLen;

    public static final short bytesUnitLen = byteLen;
    public static final short objUnitLen   = byteLen;
    public static final short textUnitLen  = byteLen;

    public static int countBytes(byte[] bs){
        return bs.length;
    }
    public static int countObj(byte[] bs){
        return countBytes(bs);
    }
    public static int countText(byte[] bs){
        return countBytes(bs);
    }
}
