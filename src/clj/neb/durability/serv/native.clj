(ns neb.durability.serv.native
  (:import (org.shisoft.neb.utils UnsafeUtils)
           (sun.misc Unsafe)))

(def ^Unsafe us UnsafeUtils/unsafe)

(defn read-bytes [reader byte-count]
  (let [^bytes byte-arr (byte-array byte-count)]
    (.read reader byte-arr)
    byte-arr))

(defn malloc-bytes [bs]
  (let [addr (.allocateMemory us (count bs))]
    (UnsafeUtils/setBytes addr bs)
    addr))

(defn read-long [reader]
  (let [addr (-> (read-bytes reader 8)
                 (malloc-bytes))
        value (.getLong us addr)]
    (.freeMemory us addr)
    value))

(defn read-int [reader]
  (let [addr (-> (read-bytes reader 4)
                 (malloc-bytes))
        value (.getInt us addr)]
    (.freeMemory us addr)
    value))