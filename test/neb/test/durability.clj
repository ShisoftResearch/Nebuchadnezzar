(ns neb.test.durability
  (:require [midje.sweet :refer :all]
            [neb.core :refer :all]
            [neb.server :refer :all]
            [neb.durability.serv.core :refer [recover-backup list-backup-ids plug-flushed-ch remove-imported-tag]]
            [neb.trunk-store :as ts :refer [backup-trunks defrag-store-trunks]]
            [neb.cell :as cell]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [clojure.core.async :as a])
  (:import (org.shisoft.neb.utils StandaloneZookeeper)
           (java.io File)
           (org.apache.commons.io FileUtils)
           (org.shisoft.neb Trunk)
           (java.util Map$Entry)))

(def trunks-size (* Integer/MAX_VALUE 1))
(def memory-size (* Integer/MAX_VALUE 2))

(let [zk (StandaloneZookeeper.)
      config {:server-name :test-server
              :port 5134
              :zk  "127.0.0.1:21817"
              :trunks-size trunks-size
              :memory-size memory-size
              :data-path   "data"
              :durability true
              :auto-backsync false
              :replication 2}
      inserted-cell-ids (atom #{})
      placr-holder "Lorem Ipsum is simply dummy text of the printing and typesetting industry."
      flush-chan (a/chan)]
  (.startZookeeper zk 21817)
  (try
    #_(facts "Durability"
           (fact "Start Server"
                 (start-server config) => anything)
           (fact "Prepare schemas"
                 (add-schema :raw-data [[:data :obj]
                                        [:num :int]]) => 0)
           (fact "Plug flush channel"
                 (plug-flushed-ch flush-chan))
           (fact "Write something"
                 (dorun
                   (pmap
                     (fn [id]
                       (let [cell-id (new-cell (str "test" id)
                                               :raw-data {:data {:str (str placr-holder id)}
                                                          :num id})]
                         (swap! inserted-cell-ids conj cell-id)
                         cell-id => anything))
                     (range 100))))
           (swap! inserted-cell-ids sort)
           (fact "Check Recover"
                 (dorun
                   (map
                     (fn [id]
                       (let [str-key (str "test" id)]
                         (read-cell str-key) => (contains {:data {:str (str placr-holder id)}
                                                           :num id
                                                           :*id* (to-id str-key)})))
                     (range 100))))
           (fact "Check dirty"
                 (let [trunks (.getTrunks ts/trunks)]
                   (doseq [trunk trunks]
                     (let [dirtyRanges (.getDirtyRanges ^Trunk trunk)]
                       (.size dirtyRanges) => 1
                       (.getValue ^Map$Entry (first dirtyRanges)) => (dec (.getAppendHeaderValue trunk))))))
           (fact "Sync trunks"
                 (backup-trunks) => anything)
           (a/<!! flush-chan)
           (a/<!! flush-chan)
           (fact "Check backedup ids"
                 (sort (set (list-backup-ids))) => (just @inserted-cell-ids :gaps-ok :in-any-order))
           (fact "Delete Everything"
                 (dorun
                   (pmap
                     (fn [id]
                       (delete-cell (str "test" id)) => anything)
                     (range 100))))
           (fact "Check Deleted"
                 (let [trunks (.getTrunks ts/trunks)]
                   (doseq [trunk trunks]
                     (let [cell-index (.getCellIndex ^Trunk trunk)]
                       (.size cell-index) => 0))))
           (fact "Recover from backups"
                 (recover-backup) => anything)
           (fact "New cells in comming"
                 (let [trunks (.getTrunks ts/trunks)]
                   (doseq [trunk trunks]
                     (let [cell-index (.getCellIndex ^Trunk trunk)]
                       (doseq [[hash meta] cell-index]
                         (cell/read-cell trunk hash) => (contains {:data anything}))
                       (.size cell-index) => #(>= % 1))))
                 (println "New cells in comming"))
           (fact "Check Recovery"
                 (dorun
                   (map
                     (fn [id]
                       (let [str-key (str "test" id)]
                         (read-cell str-key) => (contains {:data {:str (str placr-holder id)}
                                                           :num id
                                                           :*id* (to-id str-key)})))
                     (range 100)))
                 (println "Check Recovery"))
           (fact "Test with frags"
                 (remove-imported-tag)
                 (fact "Delete"
                       (dorun
                         (pmap
                           (fn [id]
                             (delete-cell (str "test" id)) => anything)
                           (range 50)))
                       (println "Test with frags - Delete"))
                 (fact "Sync trunks"
                       (backup-trunks) => anything
                       (println "Test with frags - Sync trunks"))
                 (a/<!! flush-chan)
                 (a/<!! flush-chan)
                 (fact "Delete Everything"
                       (dorun
                         (pmap
                           (fn [id]
                             (delete-cell (str "test" id)) => anything)
                           (range 50 100)))
                       (println "Test with frags - Delete Everything"))
                 (fact "Check Deleted"
                       (let [trunks (.getTrunks ts/trunks)]
                         (doseq [trunk trunks]
                           (let [cell-index (.getCellIndex ^Trunk trunk)]
                             (.size cell-index) => 0)))
                       (dorun
                         (map
                           (fn [id]
                             (let [str-key (str "test" id)]
                               (read-cell str-key) => nil))
                           (range 100))))
                 (fact "Recover from backups"
                       (recover-backup) => anything)
                 (fact "New cells in comming"
                       (let [trunks (.getTrunks ts/trunks)]
                         (doseq [trunk trunks]
                           (let [cell-index (.getCellIndex ^Trunk trunk)]
                             (doseq [[hash meta] cell-index]
                               (cell/read-cell trunk hash) => (contains {:data anything}))
                             (.size cell-index) => #(>= % 1)))))
                 (fact "Check Recovery"
                       (dorun
                         (map
                           (fn [id]
                             (let [str-key (str "test" id)]
                               (read-cell str-key) => (contains {:data {:str (str placr-holder id)}
                                                                 :num id
                                                                 :*id* (to-id str-key)})))
                           (range 50 100))))
                 (fact "Frag cells does not recorved"
                       (dorun
                         (map
                           (fn [id]
                             (let [str-key (str "test" id)]
                               (read-cell str-key) => nil))
                           (range 50))))))
    (finally
      (fact "Clear Zookeeper Server"
            (clear-zk) => anything)
      (fact "Stop Server"
            (stop-server)  => anything)
      (FileUtils/deleteDirectory (File. "data"))
      (.stopZookeeper zk))))
