package org.shisoft.neb.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by shisoft on 16-3-24.
 */
public class UnsafeUtils {
    public static final sun.misc.Unsafe unsafe;
    static {
        try {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] getBytes (long offset, int length){
        byte[] r = new byte[length];
        for (int i = 0; i < length; i++){
            r[i] = unsafe.getByte(offset + i);
        }
        return r;
    }

    public static void setBytes (long offset, byte[] bs){
        for (int i = 0; i < bs.length; i++){
            unsafe.putByte(offset + i, bs[i]);
        }
    }

    public static byte[] subBytes (byte[] bs, int offset, int length){
        byte[] r = new byte[length];
        System.arraycopy(bs, offset, r, 0, length);
        return r;
    }

}
