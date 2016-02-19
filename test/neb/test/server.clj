(ns neb.test.server
  (:require [neb.core :refer :all]
            [neb.trunk-store :as ts]
            [neb.schema :as s]
            [midje.sweet :refer :all])
  (:import [org.shisoft.neb.utils StandaloneZookeeper]))

(def trunks-size (* Integer/MAX_VALUE 1.1))
(def memory-size (* Integer/MAX_VALUE 2.2))

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
               (ts/get-trunk-store-params) => (contains {:trunks-size (fn [& _] trunks-size) :trunk-count 2}))
         (fact "Add Distributed Schemas"
               (add-schema :test-data   [[:id :int]]) => 0
               (add-schema :test-data2  [[:id :int]]) => 1)
         (fact "Remove Distributed Schemas"
               (remove-schema :test-data)   => 0
               (remove-schema :test-data2)  => 1
               (get-schemas) => empty?)
         (fact "Schema Id Reuse"
               (add-schema :raw-data [[:data :bytes]]) => 0)
         (fact "Clear Zookeeper Server"
               (clear-zk) => anything)
         (fact "Stop Server"
               (stop-server)  => anything)
         (.stopZookeeper zk)))
