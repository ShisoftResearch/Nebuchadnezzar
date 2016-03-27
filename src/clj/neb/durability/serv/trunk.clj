(ns neb.durability.serv.trunk
  (:require [neb.base :refer [cell-head-struc-map]]
            [clojure.java.io :as io])
  (:import (org.shisoft.neb.durability.io BufferedRandomAccessFile)
           (java.io InputStream)))

(defn sync-to-disk [^BufferedRandomAccessFile accessor loc ^bytes bs]
  (locking accessor
    (.seek accessor loc)
    (.write accessor bs)))

(defn recover [file-path]
  (let [^InputStream (io/input-stream file-path)]
    ))