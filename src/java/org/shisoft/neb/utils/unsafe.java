package org.shisoft.neb.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by shisoft on 16-3-24.
 */
public class unsafe {
    public static final Unsafe unsafe;
    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
