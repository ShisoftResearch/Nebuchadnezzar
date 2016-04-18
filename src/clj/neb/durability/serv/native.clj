(ns neb.durability.serv.native
  (:require [cluster-connector.utils.for-debug :refer [$ spy]])
  (:import (org.shisoft.neb.utils UnsafeUtils)
           (sun.misc Unsafe)
           (org.shisoft.neb.io type_lengths)))

(def ^Unsafe us UnsafeUtils/unsafe)

(defn read-bytes [reader byte-count]
  (let [^bytes byte-arr (byte-array byte-count)]
    (.read reader byte-arr)
    byte-arr))

(defn malloc-bytes [bs]
  (let [addr (.allocateMemory us (count bs))]
    (UnsafeUtils/setBytes addr bs)
    addr))

(defn read-long [bs offset]
  (let [addr (-> (UnsafeUtils/subBytes bs offset 8)
                 (malloc-bytes))
        value (.getLong us addr)]
    (.freeMemory us addr)
    value))

(defn read-int [bs offset]
  (let [addr (-> (UnsafeUtils/subBytes bs offset 4)
                 (malloc-bytes))
        value (.getInt us addr)]
    (.freeMemory us addr)
    value))

(defn read-int-from-bytes  [bs] (read-int bs 0))
(defn read-long-from-bytes [bs] (read-long bs 0))


(defn read-int-from-stream  [reader] (read-int-from-bytes (read-bytes reader type_lengths/intLen)))
(defn read-long-from-stream [reader] (read-long-from-bytes (read-bytes reader type_lengths/longLen)))