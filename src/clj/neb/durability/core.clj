(ns neb.durability.core
  (:require [neb.defragment :as defrag]
            [taoensso.nippy :as nippy]
            [clojure.java.io :as io]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [cluster-connector.remote-function-invocation.core :as rfi]
            [com.climate.claypoole :as cp])
  (:import (org.shisoft.neb Trunk MemoryFork Segment)
           (org.shisoft.neb.durability.io BufferedRandomAccessFile)
           (org.shisoft.neb.utils UnsafeUtils)
           (java.util.concurrent ConcurrentSkipListMap)))

;TODO: Durability for Nebuchadnezzar is still a undetermined feature.
;      The ideal design is to provide multi-master replication backend. Right now, there will be no replication.
;      Each object will only have on copy on local hard drive.
;      I thought about using memory map file, but it is uncontrolable and is also impossible to impelement replication in the future

(set! *warn-on-reflection* true)

(def server-sids (atom nil))

(defn sync-trunk [^Trunk trunk]
  (try
    (let [dirty-segments (.getDirtySegments trunk)
          sync-pool (cp/threadpool (count @server-sids) :name "Range-Sync")
          trunk-id (.getId trunk)]
      (doseq [^Segment seg dirty-segments]
        (try
          (.lockWrite seg)
          (let [base-addr (- (.getBaseAddr seg) (.getStoreAddress trunk))
                curr-addr (- (.getCurrentLoc seg) (.getStoreAddress trunk))
                seg-id (.getId seg)
                data (.getData seg)]
            (.unlockWrite seg)
            (cp/pdoseq
              sync-pool [[sn sid] @server-sids]
              (rfi/invoke sn 'neb.durability.serv.core/sync-trunk-segment
                          sid trunk-id seg-id  base-addr curr-addr data)))
          (finally (.unlockWrite seg))))
      (cp/pdoseq
        sync-pool [[sn sid] @server-sids]
        (rfi/invoke sn 'neb.durability.serv.core/sync-trunk-completed sid trunk-id)))
    (catch Exception ex
      (clojure.stacktrace/print-cause-trace ex))))