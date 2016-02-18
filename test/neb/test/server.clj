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
               (ts/get-trunk-store-params) => (contains {:trunks-size (fn [& _] trunks-size)
                                                         :trunk-count 2}))
         (fact "Add Distributed Schemas"
               (s/add-schema-distributed :test-data [[:id :int]]))
         (fact "Clear Zookeeper Server"
               (clear-zk) => anything)
         (fact "Stop Server"
               (stop-server)  => anything)
         (.stopZookeeper zk)))
