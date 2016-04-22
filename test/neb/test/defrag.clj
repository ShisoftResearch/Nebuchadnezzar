(ns neb.test.defrag
  (:require [midje.sweet :refer :all]
            [neb.schema :refer [add-schema]]
            [neb.cell :refer [new-cell read-cell delete-cell replace-cell update-cell]]
            [neb.defragment :refer [scan-trunk-and-defragment]]
            [cluster-connector.utils.for-debug :refer [spy $]])
  (:import (org.shisoft.neb Trunk Segment)))

(facts "Defragmentation"
       (let [trunk (Trunk. (Trunk/getSegSize))
             a-d {:i (int 123)}
             b-d {:l 456}
             c-d  {:f (float 1.23) :i (int 101112)}]
         (fact "create some cells"
               (add-schema :a [[:i :int]] 1) => anything
               (add-schema :b [[:l :long]] 2) => anything
               (add-schema :c [[:f :float] [:i :int]] 3) => anything
               (new-cell trunk 1 1 (int 1) a-d) => anything
               (new-cell trunk 2 1 (int 2) b-d) => anything
               (new-cell trunk 3 1 (int 3) c-d) => anything
               (new-cell trunk 4 1 (int 1) a-d) => anything
               (new-cell trunk 5 1 (int 3) c-d) => anything
               (new-cell trunk 6 1 (int 2) b-d) => anything)
         (fact "make some frags"
               (delete-cell trunk 2) => anything
               (delete-cell trunk 3) => anything
               (delete-cell trunk 5) => anything)
         (let [frag-append-head (.getCurrentLoc (first (.getSegments trunk)))]
           (fact "defrag"
                 (scan-trunk-and-defragment trunk) => anything)
           (fact "space reclaimed"
                 (< (.getCurrentLoc (first (.getSegments trunk))) frag-append-head) => true
                 (- (.getCurrentLoc (first (.getSegments trunk))) frag-append-head) => -123))
         (fact "data did not corrupted"
               (read-cell trunk 1) => (contains a-d)
               (read-cell trunk 4) => (contains a-d)
               (read-cell trunk 6) => (contains b-d))
         (fact "data can write"
               (new-cell trunk 7 1 (int 3) c-d) => anything)
         (fact "new written did not make data correpted"
               (read-cell trunk 1) => (contains a-d)
               (read-cell trunk 4) => (contains a-d)
               (read-cell trunk 6) => (contains b-d)
               (read-cell trunk 7) => (contains c-d))))