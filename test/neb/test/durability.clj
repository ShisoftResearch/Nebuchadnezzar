(ns neb.test.durability
  (:require [midje.sweet :refer :all]
            [neb.core :refer :all]
            [neb.server :refer :all]
            [neb.trunk-store :as ts :refer [backup-trunks]])
  (:import (org.shisoft.neb.utils StandaloneZookeeper)
           (java.io File)
           (org.apache.commons.io FileUtils)
           (org.shisoft.neb Trunk)
           (java.util Map$Entry)))

(def trunks-size (* Integer/MAX_VALUE 0.2))
(def memory-size (* Integer/MAX_VALUE 4))

(let [zk (StandaloneZookeeper.)
      config {:server-name :test-server
              :port 5134
              :zk  "127.0.0.1:21817"
              :trunks-size trunks-size
              :memory-size memory-size
              :data-path   "data"
              :durability true
              :auto-backsync false
              :replication 2}]
  (.startZookeeper zk 21817)
  (try
    (facts "Durability"
           (fact "Start Server"
                 (start-server config) => anything)
           (fact "Prepare schemas"
                 (add-schema :raw-data [[:data :obj]]) => 0)
           (fact "Write something"
                 (new-cell :test :raw-data {:data :abc}) => anything
                 (dorun
                   (pmap
                     (fn [id]
                       (new-cell (str "test" id)
                                 :raw-data {:data id}) => anything)
                     (range 1000))))
           (fact "Check dirty"
                 (let [trunks (.getTrunks ts/trunks)]
                   (doseq [trunk trunks]
                     (let [dirtyRanges (.getDirtyRanges ^Trunk trunk)]
                       (.size dirtyRanges) => 1
                       (.getValue ^Map$Entry (first dirtyRanges)) => (dec (.getAppendHeaderValue trunk))))))
           (fact "Make some frags"
                 (delete-cell :test) => anything)
           (fact "Sync trunks"
                 (backup-trunks) => anything)
           (fact "Delete Everything"
                 (delete-cell :test) => anything
                 (dorun
                   (pmap
                     (fn [id]
                       (delete-cell (str "test" id)) => anything)
                     (range 1000))))
           (fact "Check Deleted"
                 (let [trunks (.getTrunks ts/trunks)]
                   (doseq [trunk trunks]
                     (let [cell-index (.getCellIndex ^Trunk trunk)]
                       (.size cell-index) => 0))))
           (fact "Recover from backups"
                 ))
    (finally
      (fact "Clear Zookeeper Server"
            (clear-zk) => anything)
      (fact "Stop Server"
            (stop-server)  => anything)
      ;(FileUtils/deleteDirectory (File. "data"))
      (.stopZookeeper zk))))
