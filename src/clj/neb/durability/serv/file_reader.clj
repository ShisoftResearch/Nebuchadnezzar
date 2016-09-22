(ns neb.durability.serv.file-reader
  (:import (java.io InputStream)))

(defn read-bytes [^InputStream reader byte-count]
  (let [^bytes byte-arr (byte-array byte-count)]
    (.read reader byte-arr)
    byte-arr))

(defn skip-bytes [^InputStream reader byte-count]
  (.skip reader byte-count))