(ns neb.test.server
  (:require [neb.core :refer :all]
            [neb.trunk-store :as ts]
            [neb.schema :as s]
            [midje.sweet :refer :all]
            [cluster-connector.utils.for-debug :refer [$]])
  (:import [org.shisoft.neb.utils StandaloneZookeeper]))

(def trunks-size (* Integer/MAX_VALUE 0.2))
(def memory-size (* Integer/MAX_VALUE 4))

(defn calc-standard-deviation [coll]
  (let [ele-count (count coll)
        sum (reduce + coll)
        avg (/ sum ele-count)]
    (Math/sqrt (/ (reduce + (map (fn [n] (Math/pow (- n avg) 2)) coll)) ele-count))))

(facts "Server Tests"
       (let [zk (StandaloneZookeeper.)
             config {:server-name :test-server
                     :port 5124
                     :zk  "127.0.0.1:21817"
                     :trunks-size trunks-size
                     :memory-size memory-size
                     :data-path   "data"}]
         (.startZookeeper zk 21817)
         (fact "Start Server"
               (start-server config) => anything)
         (fact "Check server parameters"
               (ts/get-trunk-store-params) => (contains {:trunks-size (fn [& _] trunks-size) :trunk-count 20}))
         (fact "Add Distributed Schemas"
               (add-schema :test-data   [[:id :int]]) => 0
               (add-schema :test-data2  [[:id :int]]) => 1)
         (fact "Remove Distributed Schemas"
               (remove-schema :test-data)   => 0
               (remove-schema :test-data2)  => 1
               (get-schemas) => empty?)
         (fact "Schema Id Reuse"
               (add-schema :raw-data [[:data :obj]]) => 0)
         (fact "Add Cell"
               (new-cell :test :raw-data {:data :abc}) => anything)
         (fact "Batch Add Cell"
               (dorun
                 (pmap
                   (fn [id]
                     (new-cell (str "test" id)
                               :raw-data {:data id}) => anything)
                   (range 1000))))
         (fact "Check Batch Added Cell"
               (dorun
                 (pmap
                   (fn [id]
                     (read-cell (str "test" id)) => (contains {:data id}))
                   (range 1000))))
         (fact "Delete Cell"
               (delete-cell :test) => anything)
         (fact "Check Cell Distribution"
               (let [trunks-cell-count (ts/trunks-cell-count)]
                 (reduce + trunks-cell-count) => 1000
                 (calc-standard-deviation trunks-cell-count) => (fn [sd] (< sd 10))))
         (fact "Batch Add Cell"
               (batch-new-cell
                 (map
                   (fn [id]
                     [(str "test-2-" id)
                      :raw-data {:data id}])
                   (range 1000))) => anything)
         (fact "Check total cells"
               (reduce + (ts/trunks-cell-count)) => 2000)
         (fact "Check Batch Cells Added"
               (batch-read-cell-by-key
                 (map
                   #(str "test-2-" %)
                   (range 1000))) => (just (into {} (map (fn [id] [(str "test-2-" id) (contains {:data id})]) (range 1000)))))
         (fact "Batch Delete"
               (batch-delete-cell-noreply
                 (map #(str "test-2-" %) (range 1000))) => anything)
         (fact "Check Batch Delete"
               (reduce + (ts/trunks-cell-count)) => 1000)
         (fact "Clear Zookeeper Server"
               (clear-zk) => anything)
         (fact "Stop Server"
               (stop-server)  => anything)
         (.stopZookeeper zk)))
