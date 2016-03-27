(ns neb.durability.serv.file-reader)

(defn read-bytes [reader byte-count]
  (let [^bytes byte-arr (byte-array byte-count)]
    (.read reader byte-arr)
    byte-arr))