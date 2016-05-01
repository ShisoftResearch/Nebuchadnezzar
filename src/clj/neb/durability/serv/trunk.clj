(ns neb.durability.serv.trunk
  (:require [neb.header :refer [cell-head-struct cell-head-struc-map cell-head-len]]
            [neb.durability.serv.file-reader :refer [read-bytes skip-bytes]]
            [neb.durability.serv.native :refer [read-int read-long read-byte read-int-from-bytes read-long-from-bytes
                                                read-int-from-stream read-long-from-stream]]
            [neb.core :refer [new-cell-by-raw-if-newer*]]
            [neb.cell :refer [normal-cell-type]]
            [neb.durability.serv.native :refer [from-int from-long]]
            [clojure.java.io :as io]
            [cluster-connector.utils.for-debug :refer [spy $]]
            [com.climate.claypoole :as cp]
            [cluster-connector.sharding.DHT :as dht]
            [clojure.core.async :as a]
            [cluster-connector.distributed-store.core :as ds])
  (:import (org.shisoft.neb.durability.io BufferedRandomAccessFile)
           (java.io InputStream)
           (org.shisoft.neb.io type_lengths)
           (java.util UUID)
           (org.shisoft.neb.utils UnsafeUtils)))

(set! *warn-on-reflection* true)

(def file-header-size (+ type_lengths/intLen ;segment size
                         ))
(def seg-header-size (+ type_lengths/intLen ;segment append header
                        ))

(defn sync-seg-to-disk [^BufferedRandomAccessFile accessor seg-id seg-size base-addr current-addr ^bytes bs]
  (let [loc (+ base-addr file-header-size (* seg-header-size seg-id))]
    (locking accessor
      (doto accessor
        (.seek loc)
        (.write ^bytes (from-int (int current-addr)))
        (.seek (+ loc type_lengths/intLen))
        (.write bs)
        (.flush)))))

(def num-readers {:int read-int
                  :long read-long
                  :byte read-byte})

(defn read-header-bytes [segment-bytes cell-offset]
  (into {}
        (map
          (fn [[prop type]]
            (let [{:keys [offset]} (get cell-head-struc-map prop)]
              [prop ((get num-readers type) segment-bytes (+ offset cell-offset))]))
          cell-head-struct)))

(defn recover [file-path]
  (let [^InputStream reader (io/input-stream file-path)
        seg-size (read-int-from-stream reader)
        thread-size (* 10 (count (ds/get-server-list @ds/node-server-group)))
        recover-chan (a/chan thread-size)
        recover-pool (cp/threadpool thread-size :name "Recover")
        recover-seg-chan (a/chan thread-size)
        recover-seg-pool (cp/threadpool thread-size :name "Recover-Seg")]
    (try
      (while (> (.available reader) 0)
        (let [seg-append-header (read-int-from-stream reader)
              seg-data (read-bytes reader seg-size)]
          (a/>!! recover-seg-chan 0)
          (cp/future
            recover-seg-pool
            (loop [pointer 0]
              (when-not (>= pointer seg-append-header)
                (let [{:keys [partition hash cell-length cell-type version]} (read-header-bytes seg-data pointer)]
                  (if (= cell-type normal-cell-type)
                    (let [cell-id (UUID. partition hash)
                          cell-bytes (UnsafeUtils/subBytes seg-data pointer (+ cell-length cell-head-len))
                          cell-unit-len (count cell-bytes)]
                      (a/>!! recover-chan 0)
                      (try (cp/future recover-pool
                                      (new-cell-by-raw-if-newer* cell-id version cell-bytes)
                                      (a/<!! recover-chan))
                           (catch Exception ex (clojure.stacktrace/print-cause-trace ex)))
                      (recur (+ pointer cell-unit-len)))
                    (do (assert (= cell-type 2))
                        (recur (+ pointer cell-length)))))))
            (a/<!! recover-seg-chan))))
      (catch Exception ex
        (clojure.stacktrace/print-cause-trace ex))
      (finally
        (a/close! recover-chan)
        (cp/shutdown recover-pool)
        (a/close! recover-seg-chan)
        (cp/shutdown recover-seg-pool)
        (.close reader)))))

(defn list-ids [file-path]
  (let [^InputStream reader (io/input-stream file-path)
        seg-size (read-int-from-stream reader)]
    (try
      (loop [cids []]
        (if (> (.available reader) 0)
          (let [seg-append-header (read-int-from-stream reader)
                seg-data (read-bytes reader seg-size)]
            (recur (concat cids
                           (loop [pointer 0
                                  seg-cids []]
                             (if (>= pointer seg-append-header)
                               seg-cids
                               (let [{:keys [partition hash cell-length cell-type]} (read-header-bytes seg-data pointer)]
                                 (if (= cell-type normal-cell-type)
                                   (let [cell-id (UUID. partition hash)
                                         cell-bytes (UnsafeUtils/subBytes seg-data pointer (+ cell-length cell-head-len))
                                         cell-unit-len (count cell-bytes)]
                                     (recur (+ pointer cell-unit-len)
                                            (conj seg-cids cell-id)))
                                   (do (assert (= cell-type 2))
                                       (recur (+ pointer cell-length)
                                              seg-cids)))))))))
          cids))
      (catch Exception ex
        (clojure.stacktrace/print-cause-trace ex))
      (finally
        (.close reader)))))