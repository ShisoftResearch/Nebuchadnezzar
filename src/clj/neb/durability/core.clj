(ns neb.durability.core
  (:require [neb.defragment :as defrag]
            [taoensso.nippy :as nippy]
            [clojure.java.io :as io]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [cluster-connector.remote-function-invocation.core :as rfi])
  (:import (org.shisoft.neb Trunk MemoryFork)
           (org.shisoft.neb.durability.io BufferedRandomAccessFile)
           (org.shisoft.neb.utils UnsafeUtils)))

;TODO: Durability for Nebuchadnezzar is still a undetermined feature.
;      The ideal design is to provide multi-master replication backend. Right now, there will be no replication.
;      Each object will only have on copy on local hard drive.
;      I thought about using memory map file, but it is uncontrolable and is also impossible to impelement replication in the future

(set! *warn-on-reflection* true)

(def server-sids (atom nil))

(defn sync-range [^Trunk trunk start end]
  (let [^MemoryFork mf (.getMemoryFork trunk)
        store-addr (.getStoreAddress trunk)
        bs (.getBytes mf (+ store-addr start) (+ store-addr end))
        trunk-id (.getId trunk)]
    (dorun
      (pmap
        (fn [[sn sid]]
          (rfi/invoke sn 'neb.durability.serv.core/sync-trunk
                      sid trunk-id start bs))
        @server-sids))))

(defn finish-trunk-sync [^Trunk trunk tail-loc timestamp]
  (let [trunk-id (.getId trunk)]
    (dorun
      (pmap
        (fn [[sn sid]]
          (rfi/invoke sn 'neb.durability.serv.core/finish-trunk-sync
                      sid trunk-id tail-loc timestamp))
        @server-sids))))

(defn sync-trunk [^Trunk trunk]
  (.writeLock trunk)
  (try
    (defrag/scan-trunk-and-defragment trunk)
    (if-not (empty? (.getFragments trunk))
      (println "WARNING: Defrag for sync not succeed")
      (let [dirty-ranges  (.clone (.getDirtyRanges trunk))
            append-header (.getAppendHeaderValue trunk)
            ^MemoryFork mf (.fork trunk)
            timestamp (System/nanoTime)]
        (.clear (.getDirtyRanges trunk))
        (.writeUnlock trunk)
        (loop [pos 0]
          (let [d-range (.ceilingEntry dirty-ranges pos)]
            (if-not d-range
              (when (> (.size dirty-ranges) 0)
                (finish-trunk-sync trunk append-header timestamp))
              (do (let [start (.getKey d-range)
                        end (min (.getValue d-range) (dec append-header))]
                    (sync-range trunk start end))
                  (recur (inc (.getValue d-range)))))))
        (.release mf)))
    (catch Exception ex
      (clojure.stacktrace/print-cause-trace ex))
    (finally
      (.writeUnlock trunk))))