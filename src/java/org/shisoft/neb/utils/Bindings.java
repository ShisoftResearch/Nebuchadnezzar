package org.shisoft.neb.utils;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

/**
 * Created by shisoft on 16-4-11.
 */
public class Bindings {
    public static IFn readCellHash = Clojure.var("neb.header", "read-cell-hash");
    public static IFn readCellLength = Clojure.var("neb.header", "read-cell-length");
    public static IFn cellHeadLen = Clojure.var("neb.header", "get-cell-head-len");
}
