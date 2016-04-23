(ns neb.test.trunk
  (:require [midje.sweet :refer :all]
            [neb.schema :refer [add-schema]]
            [neb.cell :refer [new-cell read-cell delete-cell replace-cell update-cell read-cell-headers]]
            [cluster-connector.utils.for-debug :refer [spy $]])
  (:import (org.shisoft.neb Trunk SchemaStore)
           (org.shisoft.neb.io CellReader CellWriter Reader type_lengths)))

(def ttrunk (atom nil))
(def simple-scheme [[:int-value :int]])
(def simple-scheme-data {:int-value (rand-int Integer/MAX_VALUE)})
(def simple-scheme-data-replacement {:int-value (rand-int Integer/MIN_VALUE)})

(def compound-scheme [[:int-value :int] [:long-value :long] [:char-value :char] [:text-value :text]
                      [:len-offset :int]])
(def compound-scheme-data
  {:int-value (rand-int Integer/MAX_VALUE) :long-value Long/MAX_VALUE
   :char-value \测
   :text-value "Google DeepMind开发的AlphaGo成为首个在公平比赛击败职业围棋选手的电脑围棋程式。"
   :len-offset 15})
(def compound-scheme-data-replacement
  {:int-value (rand-int Integer/MAX_VALUE) :long-value Long/MAX_VALUE
   :char-value \测
   :text-value "Google DeepMind开发的AlphaGo成为首个在公平比赛击败职业围棋选手的电脑围棋程式。 Нет никого, кто любил бы боль саму по себе, кто искал бы её и кто хотел бы иметь её просто потому, что это боль."
   :len-offset 15})
(def compound-scheme-data-shrinked-replacement
  {:int-value (rand-int Integer/MAX_VALUE) :long-value Long/MAX_VALUE
   :char-value \测
   :text-value "Google DeepMind"
   :len-offset 15})

(facts "trunk test"
       (fact "memory init"
             (reset! ttrunk (Trunk. (Trunk/getSegSize))) => anything)
       (fact "define simple scheme"
             (add-schema :test-schema simple-scheme 1) => anything)
       (fact "write cell with simple shceme"
             (new-cell @ttrunk 1 1 (int 1) simple-scheme-data) => anything)
       (fact "cell headers"
             (read-cell-headers @ttrunk 1) => {:cell-type 1, :cell-length 4, :partition 1, :hash 1, :schema-id 1, :version 0})
       (fact "read cell with simple scheme"
             (read-cell @ttrunk 1) => (contains simple-scheme-data))
       (fact "define compound scheme"
             (add-schema :test-schema2 compound-scheme 2) => anything)
       (fact "write cell with compound shceme"
             (new-cell @ttrunk 2 1 (int 2) compound-scheme-data) => anything)
       (fact "Write cell with wrong data types"
             (new-cell @ttrunk 3 1 (int 1) {:int-value "abc"}) => (throws IllegalArgumentException))
       (fact "read cell with compound scheme"
             (read-cell @ttrunk 2) => (contains compound-scheme-data))
       (fact "delete cell"
             (delete-cell @ttrunk 2) => anything)
       (fact "deleted cell cannot been read"
             (read-cell @ttrunk 2) => nil)
       (fact "replace cell"
             (replace-cell @ttrunk 1 simple-scheme-data-replacement) => simple-scheme-data-replacement)
       (fact "cell headers"
             (read-cell-headers @ttrunk 1) => {:cell-type 1, :cell-length 4, :partition 1, :hash 1, :schema-id 1, :version 1})
       (fact "cell should been replaced"
             (read-cell @ttrunk 1) => (contains simple-scheme-data-replacement))
       (fact "shrinked replace cell"
             (new-cell @ttrunk 2 1 (int 2) compound-scheme-data) => anything
             (replace-cell @ttrunk 2 compound-scheme-data-shrinked-replacement) => compound-scheme-data-shrinked-replacement)
       (fact "shrinked cell should been replaced"
             (read-cell @ttrunk 2) => (contains compound-scheme-data-shrinked-replacement))
       (fact "overflow replace cell"
             (replace-cell @ttrunk 2 compound-scheme-data-replacement) => compound-scheme-data-replacement)
       (fact "overflow cell should been replaced"
             (read-cell @ttrunk 2) => (contains compound-scheme-data-replacement))
       (fact "update cell"
             (update-cell @ttrunk 2 'clojure.core/update :len-offset dec) => (contains {:len-offset (dec (:len-offset compound-scheme-data-replacement))}))
       (fact "cell updated"
             (read-cell @ttrunk 2) => (contains {:len-offset (dec (:len-offset compound-scheme-data-replacement))}))
       (fact "dispose"
             (.dispose @ttrunk) => truthy))
