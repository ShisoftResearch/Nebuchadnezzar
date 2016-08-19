(ns neb.test.concurrent
  (:require [midje.sweet :refer :all]
            [neb.schema :refer [add-schema]]
            [neb.cell :refer [new-cell read-cell delete-cell replace-cell update-cell read-cell-headers]]
            [com.climate.claypoole :as cp]
            [cluster-connector.utils.for-debug :refer [spy $]])
  (:import (org.shisoft.neb Trunk)
           (org.shisoft.neb.io CellReader CellWriter Reader type_lengths Writer)
           (java.util UUID)
           (java.util.concurrent.locks ReentrantReadWriteLock$ReadLock ReentrantReadWriteLock ReentrantReadWriteLock$WriteLock)))

(defn update-test [cell]
  (-> cell
      (update :num inc)
      (assoc :str (str (rand-int Integer/MAX_VALUE)))))

#_(fact "Concurrent test"
      (let [trunk (Trunk. (Trunk/getSegSize))
            pool (cp/threadpool 64)]

        (fact "Schema"
              (add-schema :cctest [[:num :int] [:str :text]] 0) => anything)

        (fact "Insert"
              (new-cell trunk 0 0 0 {:num 0 :str ""}) => anything)

        (fact "Reset"
              (replace-cell trunk 0 {:num (rand-int Integer/MAX_VALUE) :str (str (rand-int Integer/MAX_VALUE))}) => anything)

        (fact "Concurrent Reset"
              (let [check-results
                    (cp/pfor
                      pool
                      [i (range 1000)]
                      (replace-cell trunk 0 {:num i :str (str i)}))
                    addresses
                    (for [result* check-results]
                      (get-in result* [:*result* :addr]))
                    original-addresses
                    (for [result* check-results]
                      (get-in result* [:*result* :orig-addr]))]
                (doseq
                  [result* check-results]
                  result* => (contains {:num number? :str string? :*result* anything}))

                (fact "New Addr"
                      (count (frequencies addresses)) => 1000)
                (fact "Original Addr"
                      (count (frequencies original-addresses)) => 1000)))

        (fact "Reset Version Check"
              (read-cell trunk 0) => (contains {:*version* 1001}))

        (fact "Concurrent Update"
              (cp/pdoseq
                pool
                [i (range 1000)]
                (update-cell trunk 0 'neb.test.concurrent/update-test) => anything))

        (cp/shutdown pool)
        (.dispose trunk)))
