(ns neb.durability.core
  (:require [neb.defragment :as defrag]
            [taoensso.nippy :as nippy]
            [clojure.java.io :as io]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [cluster-connector.remote-function-invocation.core :as rfi]
            [com.climate.claypoole :as cp])
  (:import (org.shisoft.neb Trunk MemoryFork)
           (org.shisoft.neb.durability.io BufferedRandomAccessFile)
           (org.shisoft.neb.utils UnsafeUtils)
           (java.util.concurrent ConcurrentSkipListMap)))

;TODO: Durability for Nebuchadnezzar is still a undetermined feature.
;      The ideal design is to provide multi-master replication backend. Right now, there will be no replication.
;      Each object will only have on copy on local hard drive.
;      I thought about using memory map file, but it is uncontrolable and is also impossible to impelement replication in the future

(set! *warn-on-reflection* true)

(def server-sids (atom nil))

(defn sync-range [^Trunk trunk start end]
  (let [^MemoryFork mf (.getMemoryFork trunk)
        trunk-id (.getId trunk)
        pool (cp/threadpool (count @server-sids)
                            :name "Range-Sync")]
    (.syncBytes
      mf start end
      (fn [^bytes bs ^Long start]
        (cp/pdoseq
          pool [[sn sid] @server-sids]
          (rfi/invoke sn 'neb.durability.serv.core/sync-trunk
                      sid trunk-id (- start (.getStoreAddress trunk)) bs))))
    (cp/shutdown pool)))

(defn finish-trunk-sync [^Trunk trunk tail-loc timestamp]
  (let [trunk-id (.getId trunk)
        pool (cp/threadpool (count @server-sids)
                            :name "Finish-Sync")]
    (cp/pdoseq
      pool [[sn sid] @server-sids]
      (rfi/invoke sn 'neb.durability.serv.core/finish-trunk-sync
                  sid trunk-id tail-loc timestamp))
    (cp/shutdown pool)))

(defn sync-trunk [^Trunk trunk]
  (try
    (let [^ConcurrentSkipListMap trunk-dirty-ranges (.getDirtyRanges trunk)
          ^MemoryFork mf (.fork trunk)
          dirty-ranges  (.clone trunk-dirty-ranges)
          timestamp (System/currentTimeMillis)]
      (loop [pos 0]
        (let [d-range (.ceilingEntry dirty-ranges pos)]
          (when d-range
            (.remove trunk-dirty-ranges
                     (.getKey d-range) (.getValue d-range)))
          (if (not d-range)
            (when (> (.size dirty-ranges) 0)
              #_(finish-trunk-sync trunk append-header timestamp))
            (do (let [start (.getKey d-range)
                      end (.getValue d-range)]
                  (sync-range trunk start end))
                (recur (inc (.getValue d-range)))))))
      (.release mf))
    (catch Exception ex
      (clojure.stacktrace/print-cause-trace ex))))