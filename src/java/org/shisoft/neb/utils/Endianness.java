package org.shisoft.neb.utils;

import sun.misc.Unsafe;

public class Endianness {

    public static boolean isBigEndian (){
        Unsafe unsafe = UnsafeUtils.unsafe;
        long address = unsafe.allocateMemory(2);
        try {
            short number = 1;
            unsafe.putShort(address, number);
            return (unsafe.getByte(address) == 0);
        } finally {
            unsafe.freeMemory(address);
        }
    }

    public static void main(String[] args) {
        if (isBigEndian())
            System.out.println("Big Endian");
        else
            System.out.println("Little Endian");
    }


}