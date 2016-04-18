(ns neb.durability.serv.trunk
  (:require [neb.header :refer [cell-head-struct cell-head-struc-map cell-head-len]]
            [neb.durability.serv.file-reader :refer [read-bytes skip-bytes]]
            [neb.durability.serv.native :refer [read-int read-long read-int-from-bytes read-long-from-bytes
                                                read-int-from-stream read-long-from-stream]]
            [neb.core :refer [new-cell-by-raw*]]
            [neb.cell :refer [normal-cell-type]]
            [clojure.java.io :as io]
            [cluster-connector.utils.for-debug :refer [spy $]]
            [com.climate.claypoole :as cp])
  (:import (org.shisoft.neb.durability.io BufferedRandomAccessFile)
           (java.io InputStream)
           (org.shisoft.neb.io type_lengths)
           (java.util UUID)
           (com.google.common.primitives Ints)
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
        (.write (Ints/toByteArray (- current-addr base-addr)))
        (.seek (+ loc type_lengths/intLen))
        (.write bs)
        (.flush)))))

(def num-readers {:int read-int
                  :long read-long})

(defn read-header-bytes [header-bytes]
  (into {}
        (map
          (fn [[prop type]]
            (let [{:keys [offset]} (get cell-head-struc-map prop)]
              [prop ((get num-readers type) header-bytes offset)]))
          cell-head-struct)))

(defn recover [file-path]
  (let [^InputStream reader (io/input-stream file-path)
        seg-size (read-int-from-stream reader)]
    (try
      (while (> (.available reader) 0)
        (let [seg-append-header (read-int-from-stream reader)
              seg-data (read-bytes reader seg-size)]
          (loop [data seg-data
                 pointer 0]
            (when-not (or (empty? data) (< pointer seg-append-header))
              (let [header-bytes (UnsafeUtils/subBytes data 0 cell-head-len)
                    {:keys [partition hash cell-length cell-type]} (read-header-bytes header-bytes)
                    data-len (count data)]
                (if (= cell-type normal-cell-type)
                  (let [cell-id (UUID. partition hash)
                        cell-bytes (UnsafeUtils/subBytes data 0 (+ cell-length cell-head-len))
                        cell-unit-len (count cell-bytes)
                        cell-tail (- data-len cell-unit-len)]
                    (new-cell-by-raw* cell-id cell-bytes)
                    (recur (UnsafeUtils/subBytes data cell-unit-len cell-tail)
                           (+ pointer cell-unit-len)))
                  (do (assert (= cell-type 2))
                      (recur (UnsafeUtils/subBytes data cell-length (- data-len cell-length))
                             (+ pointer (- data-len cell-length))))))))))
      (catch Exception ex
        (clojure.stacktrace/print-cause-trace ex))
      (finally
        (.close reader)))))

(defn list-ids [file-path]
  (let [^InputStream reader (io/input-stream file-path)
        seg-size (read-int-from-stream reader)]
    (try
      (while (> (.available reader) 0)
        (let [seg-append-header (read-int-from-stream reader)
              seg-data (read-bytes reader seg-size)]
          (loop [data seg-data
                 pointer 0
                 cids []]
            (when-not (or (empty? data) (< pointer seg-append-header))
              (let [header-bytes (UnsafeUtils/subBytes data 0 cell-head-len)
                    {:keys [partition hash cell-length cell-type]} (read-header-bytes header-bytes)
                    data-len (count data)]
                (if (= cell-type normal-cell-type)
                  (let [cell-id (UUID. partition hash)
                        cell-bytes (UnsafeUtils/subBytes data 0 (+ cell-length cell-head-len))
                        cell-unit-len (count cell-bytes)
                        cell-tail (- data-len cell-unit-len)]
                    (recur (UnsafeUtils/subBytes data cell-unit-len cell-tail)
                           (+ pointer cell-unit-len)
                           (conj cids cell-id)))
                  (do (assert (= cell-type 2))
                      (recur (UnsafeUtils/subBytes data cell-length (- data-len cell-length))
                             (+ pointer (- data-len cell-length))
                             cids))))))))
      (catch Exception ex
        (clojure.stacktrace/print-cause-trace ex))
      (finally
        (.close reader)))))