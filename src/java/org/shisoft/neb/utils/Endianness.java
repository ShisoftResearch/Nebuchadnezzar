package org.shisoft.neb.utils;

import java.lang.reflect.Field;
import sun.misc.Unsafe;

public class Endianness {
    private static Unsafe getUnsafe() {
        Unsafe unsafe = null;
        try {
            Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
        } catch (Exception e) {}
        return unsafe;
    }

    public static void main(String[] args) {
        Unsafe unsafe = getUnsafe();
        long address = unsafe.allocateMemory(2);
        short number = 1;
        unsafe.putShort(address, number);
        if (unsafe.getByte(address) == 0)
            System.out.println("Big Endian");
        else
            System.out.println("Little Endian");
        unsafe.freeMemory(address);
    }
}