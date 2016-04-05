(ns neb.durability.serv.trunk
  (:require [neb.header :refer [cell-head-struct cell-head-struc-map cell-head-len]]
            [neb.durability.serv.file-reader :refer [read-bytes]]
            [neb.durability.serv.native :refer [read-int read-long]]
            [neb.core :refer [new-cell-by-raw*]]
            [clojure.java.io :as io]
            [cluster-connector.utils.for-debug :refer [spy $]]
            [com.climate.claypoole :as cp])
  (:import (org.shisoft.neb.durability.io BufferedRandomAccessFile)
           (java.io InputStream)
           (java.util UUID)))

(set! *warn-on-reflection* true)

(defn sync-to-disk [^BufferedRandomAccessFile accessor loc ^bytes bs]
  (locking accessor
    (.seek accessor loc)
    (.write accessor bs)))

(def num-readers {:int read-int
                  :long read-long})

(defn read-header-bytes [header-bytes]
  (into {}
    (map
      (fn [[prop type]]
        (let [{:keys [offset]} (get cell-head-struc-map prop)]
          [prop ((get num-readers type) header-bytes offset)]))
      cell-head-struct)))

(defn assemble-cell-bytes [header-bytes body-bytes]
  (let [body-length (count body-bytes)
        r (byte-array (+ cell-head-len body-length))]
    (System/arraycopy header-bytes 0 r 0 cell-head-len)
    (System/arraycopy body-bytes 0 r cell-head-len body-length)
    r))

(defn recover [file-path]
  (let [^InputStream reader (io/input-stream file-path)]
    (try
      (while (> (.available reader) 0)
        (let [header-bytes (read-bytes reader cell-head-len)
              {:keys [partition hash cell-length]} (read-header-bytes header-bytes)
              cell-id (UUID. partition hash)
              body-bytes (read-bytes reader cell-length)
              cell-bytes (assemble-cell-bytes header-bytes body-bytes)]
          (new-cell-by-raw* cell-id cell-bytes)))
      (catch Exception ex
        (clojure.stacktrace/print-cause-trace ex))
      (finally
        (.close reader)))))

(defn list-ids [file-path]
  (let [^InputStream reader (io/input-stream file-path)]
    (try
      (loop [ids []]
        (if (> (.available reader) 0)
          (let [header-bytes (read-bytes reader cell-head-len)
                {:keys [partition hash cell-length]} (read-header-bytes header-bytes)
                cell-id (UUID. partition hash)
                body-bytes (read-bytes reader cell-length)
                cell-bytes (assemble-cell-bytes header-bytes body-bytes)]
            (recur (conj ids cell-id)))
          ids))
      (finally
        (.close reader)))))