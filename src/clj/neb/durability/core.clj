(ns neb.durability.core
  (:require [neb.defragment :as defrag]
            [taoensso.nippy :as nippy]
            [clojure.java.io :as io]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [cluster-connector.remote-function-invocation.core :as rfi]
            [com.climate.claypoole :as cp])
  (:import (org.shisoft.neb Trunk Segment)))

;TODO: Durability for Nebuchadnezzar is still a undetermined feature.

(set! *warn-on-reflection* true)

(def server-sids (atom nil))

(defn sync-trunk [^Trunk trunk]
  (try
    (let [dirty-segments (.getDirtySegments trunk)
          sync-pool (cp/threadpool (count @server-sids) :name "Backup-Remote")
          trunk-id (.getId trunk)]
      (doseq [^Segment seg dirty-segments]
        (when (and (.isDirty seg) (not (.isWriteLocked (.getLock seg))))
          (.lockWrite seg)
          (try
            (let [base-addr (- (.getBaseAddr seg) (.getStoreAddress trunk))
                  curr-addr (- (.getCurrentLoc seg) (.getBaseAddr seg))
                  seg-id (.getId seg)
                  data (.getData seg)
                  is-dirty? (.isDirty seg)]
              (.setClean seg)
              (.unlockWrite seg)
              (when is-dirty?
                (cp/pdoseq
                  sync-pool [[sn sid] @server-sids]
                  (rfi/invoke sn 'neb.durability.serv.core/sync-trunk-segment
                              sid trunk-id seg-id  base-addr curr-addr data))))
            (finally (.unlockWrite seg)))))
      (cp/pdoseq
        sync-pool [[sn sid] @server-sids]
        (rfi/invoke sn 'neb.durability.serv.core/sync-trunk-completed sid trunk-id))
      (cp/shutdown sync-pool))
    (catch Exception ex
      (clojure.stacktrace/print-cause-trace ex))))