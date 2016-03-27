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
}
