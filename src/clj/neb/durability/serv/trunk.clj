(ns neb.durability.serv.trunk
  (:require [neb.header :refer [cell-head-struct cell-head-struc-map cell-head-len]]
            [neb.durability.serv.file-reader :refer [read-bytes skip-bytes]]
            [neb.durability.serv.native :refer [read-int read-long]]
            [neb.core :refer [new-cell-by-raw*]]
            [neb.cell :refer [normal-cell-type]]
            [clojure.java.io :as io]
            [cluster-connector.utils.for-debug :refer [spy $]]
            [com.climate.claypoole :as cp])
  (:import (org.shisoft.neb.durability.io BufferedRandomAccessFile)
           (java.io InputStream)
           (org.shisoft.neb.io type_lengths)
           (java.util UUID)
           (com.google.common.primitives Ints)))

(set! *warn-on-reflection* true)

(def file-header-size (+ type_lengths/intLen ;segment size
                         ))
(def seg-header-size (+ type_lengths/intLen ;segment append header
                        ))

(defn sync-seg-to-disk [^BufferedRandomAccessFile accessor seg-id seg-size base-addr current-addr ^bytes bs]
  (let [loc (+ base-addr file-header-size (* seg-header-size seg-id))]
    (locking accessor
      (.seek accessor loc)
      (.write accessor (Ints/toByteArray (- current-addr base-addr)))
      (.seek accessor (+ loc type_lengths/intLen))
      (.write accessor bs)
      (.flush))))

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
              {:keys [partition hash cell-length cell-type]} (read-header-bytes header-bytes)]
          (if (= cell-type normal-cell-type)
            (let [cell-id (UUID. partition hash)
                  body-bytes (read-bytes reader cell-length)
                  cell-bytes (assemble-cell-bytes header-bytes body-bytes)]
              (new-cell-by-raw* cell-id cell-bytes))
            (skip-bytes reader (- cell-length cell-head-len)))))
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