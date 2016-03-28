(ns neb.test.durability
  (:require [midje.sweet :refer :all]
            [neb.core :refer :all]
            [neb.server :refer :all])
  (:import (org.shisoft.neb.utils StandaloneZookeeper)))

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
    (facts "Submit Changes"
           (fact "Start Server"
                 (start-server config) => anything)
           (fact "Write something"
                 ))
    (finally
      (fact "Clear Zookeeper Server"
            (clear-zk) => anything)
      (fact "Stop Server"
            (stop-server)  => anything)
      (.stopZookeeper zk))))
