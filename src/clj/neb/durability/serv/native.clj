(ns neb.durability.serv.native
  (:require [cluster-connector.utils.for-debug :refer [$ spy]])
  (:import (org.shisoft.neb.utils UnsafeUtils)
           (sun.misc Unsafe)
           (org.shisoft.neb.io type_lengths)
           (java.io InputStream)))

(set! *warn-on-reflection* true)

(def ^Unsafe us UnsafeUtils/unsafe)

(defn read-bytes [^InputStream reader byte-count]
  (let [^bytes byte-arr (byte-array byte-count)]
    (.read reader byte-arr)
    byte-arr))

(defn malloc-bytes [bs]
  (let [addr (.allocateMemory us (count bs))]
    (UnsafeUtils/setBytes addr bs)
    addr))

(defn dealloc [addr]
  (.freeMemory us addr))

(defn read-long-from-mem [addr]
  (.getLong us addr))

(defn read-int-from-mem [addr]
  (.getInt us addr))

(defn read-byte-from-mem [addr] (.getByte us addr))

(defn read-long [bs offset]
  (let [addr (-> (UnsafeUtils/subBytes bs offset type_lengths/longLen)
                 (malloc-bytes))
        value (read-long-from-mem addr)]
    (dealloc addr)
    value))

(defn read-int [bs offset]
  (let [addr (-> (UnsafeUtils/subBytes bs offset type_lengths/intLen)
                 (malloc-bytes))
        value (read-int-from-mem addr)]
    (dealloc addr)
    value))

(defn read-byte [bs offset] (get bs offset))

(defn read-int-from-bytes  [bs] (read-int bs 0))
(defn read-long-from-bytes [bs] (read-long bs 0))


(defn read-int-from-stream  [reader] (read-int-from-bytes (read-bytes reader type_lengths/intLen)))
(defn read-long-from-stream [reader] (read-long-from-bytes (read-bytes reader type_lengths/longLen)))

(defn from-int [i]
  (let [i (int i)
        addr (.allocateMemory us type_lengths/intLen)
        _ (.putInt us addr i)
        bs (UnsafeUtils/getBytes addr type_lengths/intLen)]
    (dealloc addr)
    bs))

(defn from-long [l]
  (let [addr (.allocateMemory us type_lengths/longLen)
        _ (.putLong us addr l)
        bs (UnsafeUtils/getBytes addr type_lengths/longLen)]
    (dealloc addr)
    bs))