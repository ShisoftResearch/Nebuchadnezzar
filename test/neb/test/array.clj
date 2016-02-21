(ns neb.test.array
  (:require [midje.sweet :refer :all]
            [neb.schema :refer [add-schema]]
            [neb.cell :refer [new-cell read-cell delete-cell replace-cell update-cell]])
  (:import (org.shisoft.neb trunk)))

(fact "Test Array"
      (let [trunk (trunk. 5000000)]
        (fact "Array Schema"
              (add-schema :array-schema [[:arr :long-array]] 1) => anything)
        (fact "Write Cell With Array"
              (new-cell trunk 1 (int 1) {:arr (range 100)}) => anything)
        (fact "Read Cell With Array"
              (read-cell trunk 1) => (fn [m] (= m {:arr (vec (range 100))})))
        (.dispose trunk)))