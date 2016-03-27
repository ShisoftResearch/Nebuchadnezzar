(ns neb.durability.serv.trunk
  (:require [neb.base :refer [cell-head-struc-map]])
  (:import (org.shisoft.neb.durability.io BufferedRandomAccessFile)))

(defn sync-to-disk [^BufferedRandomAccessFile accessor loc ^bytes bs]
  (locking accessor
    (.seek accessor loc)
    (.write accessor bs)))

(defn recover [file-path]
  )