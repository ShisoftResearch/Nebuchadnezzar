(ns neb.test.nested
  (:require [midje.sweet :refer :all]
            [neb.schema :refer [add-schema]]
            [neb.cell :refer [new-cell read-cell delete-cell replace-cell update-cell]]
            [cluster-connector.utils.for-debug :refer [$]])
  (:import (org.shisoft.neb trunk)))

(fact "Test Internal Array"
      (let [trunk (trunk. 5000000)]
        (fact "Array Schema"
              (add-schema :array-schema [[:arr :long-array]] 1) => anything)
        (fact "Write Cell With Array"
              (new-cell trunk 1 (int 1) {:arr (range 100)}) => anything)
        (fact "Read Cell With Array"
              (read-cell trunk 1) => (contains {:arr (vec (range 100))}))
        (.dispose trunk)))

(fact "Test Array"
      (let [trunk (trunk. 5000000)]
        (fact "Array Schema"
              (add-schema :array-schema [[:arr [:ARRAY :long]]] 1) => anything)
        (fact "Write Cell With Array"
              (new-cell trunk 1 (int 1) {:arr (range 100)}) => anything)
        (fact "Read Cell With Array"
              (read-cell trunk 1) => (contains {:arr (vec (range 100))}))
        (.dispose trunk)))

(fact "Test Nested Array"
      (let [trunk (trunk. 5000000)]
        (fact "Array Schema"
              (add-schema :array-schema [[:arr [:ARRAY [:ARRAY :long]]]] 1) => anything)
        (fact "Write Cell With Array"
              (new-cell trunk 1 (int 1) {:arr (repeat 10 (range 10))}) => anything)
        (fact "Read Cell With Array"
              (read-cell trunk 1) => (contains {:arr (vec (repeat 10 (vec (range 10))))}))
        (.dispose trunk)))

(fact "Test Map"
      (let [trunk (trunk. 5000000)]
        (fact "Map Schema"
              (add-schema :array-schema [[:map [[:a :long] [:b :long]]]] 1) => anything)
        (fact "Write Cell With Map"
              (new-cell trunk 1 (int 1) {:map {:a 1 :b 2}}) => anything)
        (fact "Read Cell With Map"
              (read-cell trunk 1) => (contains {:map {:a 1 :b 2}}))
        (.dispose trunk)))

(fact "Test Map Array"
      (let [trunk (trunk. 5000000)]
        (fact "Map Schema"
              (add-schema :array-schema [[:map [[:a :long] [:b [:ARRAY :long]]]]] 1) => anything)
        (fact "Write Cell With Map"
              (new-cell trunk 1 (int 1) {:map {:a 1 :b (range 10)}}) => anything)
        (fact "Read Cell With Map"
              (read-cell trunk 1) => (contains {:map {:a 1 :b (range 10)}}))
        (.dispose trunk)))

(fact "Test Array Map"
      (let [trunk (trunk. 5000000)]
        (fact "Map Schema"
              (add-schema :array-schema [[:map [[:a :long] [:b [:ARRAY [[:arr-map :long]]]]]]] 1) => anything)
        (fact "Write Cell With Map"
              (new-cell trunk 1 (int 1) {:map {:a 1 :b (repeat 10 {:arr-map 5})}}) => anything)
        (fact "Read Cell With Map"
              (read-cell trunk 1) => (contains {:map {:a 1 :b (repeat 10 {:arr-map 5})}}))
        (.dispose trunk)))