(ns neb.test.unsafe-longlong-hashmap
  (:require [midje.sweet :refer :all]
            [cluster-connector.utils.for-debug :refer [spy $]])
  (:import (org.shisoft.neb.utils UnsafeConcurrentLongLongHashMap)))

(fact "Unsafe concurrent long->long hash map"
      (println "Testing ucllmap")
      (let [^UnsafeConcurrentLongLongHashMap ucll-map (UnsafeConcurrentLongLongHashMap.)]

        (fact "Put"

              (.put ucll-map 1000 1001) => pos?
              (.put ucll-map 2000 2001) => pos?

              (fact "Batch"
                    (doall
                      (pmap
                        (fn [v]
                          (.put ucll-map v (inc v)) => pos?)
                        (range 1 100000)))))

        (fact "Get"
              (fact "Batch"
                    (doall
                      (pmap
                        (fn [v]
                          (.get ucll-map v) => (inc v))
                        (range 1 100000)))))

        (fact "Buckets Resized"
              (.getBuckets ucll-map) => #(> % (.getInitialBuckets ucll-map)))

        (fact "Clear"
              (.clear ucll-map) => anything
              (.size ucll-map) => 0
              (doall
                (pmap
                  (fn [v]
                    (.get ucll-map v) => -1)
                  (range 1 100000))))

        (println "Disposing ucllmap")
        (.dispose ucll-map) => anything
        (println "Testing ucllmap done")))