package org.shisoft.neb.io;

/**
 * Created by shisoft on 29/9/14.
 */
public class type_lengths {
    public static final short charLen = 2;
    public static final short intLen = 4;
    public static final short longLen = 8;
    public static final short booleanLen = 1;
    public static final short shortLen = 2;
    public static final short ushortLen = 2;
    public static final short byteLen = 1;
    public static final short floatLen = 4;
    public static final short doubleLen = 8;
    public static final short uuidLen = 2 * longLen;
    public static final short cidLen = 16;
    public static final short pos2dLen = 2 * doubleLen;
    public static final short pos3dLen = 3 * doubleLen;
    public static final short pos4dLen = 4 * doubleLen;
    public static final short geoLen = 2 * floatLen;
    public static final short dateLen = longLen;
}
