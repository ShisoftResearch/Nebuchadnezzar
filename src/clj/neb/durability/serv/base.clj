(ns neb.durability.serv.base
  (:import (com.google.common.primitives Longs Ints)))

(defn read-bytes [reader byte-count]
  (let [^bytes byte-arr (byte-array byte-count)]
    (.read reader byte-arr)
    byte-arr))

(defn read-long [reader]
  (Longs/fromByteArray (read-bytes reader 8)))

(defn read-int [reader]
  (Ints/fromByteArray (read-bytes reader 4)))