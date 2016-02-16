(ns neb.test.server
  (:require [neb.core :refer :all]
            [midje.sweet :refer :all])
  (:import [org.shisoft.neb.utils StandaloneZookeeper]))

(def server-config
  {:server-name "test server"})

(facts "Server Tests"
       (let [zk (StandaloneZookeeper.)
             config {:server-name :test-server
                     :port 2115
                     :zk  "127.0.0.1:21817"
                     :trunks-size (* Integer/MAX_VALUE 1.1)
                     :memory-size (* Integer/MAX_VALUE 2.2)
                     :data-path   "data"}]
         (.startZookeeper zk 21817)
         (fact "Start Server"
               (start-server config) => anything)
         (fact "Stop Server"
               (stop-server)  => anything)
         (.stopZookeeper zk)))
