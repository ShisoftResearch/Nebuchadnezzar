(ns neb.test.trunk
  (:require [midje.sweet :refer :all]
            [neb.schema :refer [add-scheme]]
            [neb.cell :refer [new-cell read-cell delete-cell replace-cell update-cell]]
            [cluster-connector.utils.for-debug :refer [spy $]])
  (:import (org.shisoft.neb trunk schemaStore)
           (org.shisoft.neb.io cellReader cellWriter reader type_lengths)))

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
             (reset! ttrunk (trunk. 50000)) => anything)
       (fact "define simple scheme"
             (add-scheme :test-schema simple-scheme 1) => anything)
       (fact "write cell with simple shceme"
             (time (do (new-cell @ttrunk 1 (int 1) simple-scheme-data))) => anything)
       (fact "read cell with simple scheme"
             (time (do (read-cell @ttrunk 1))) => simple-scheme-data)
       (fact "define compound scheme"
             (time (do (add-scheme :test-schema2 compound-scheme 2))) => anything)
       (fact "write cell with compound shceme"
             (time (do (new-cell @ttrunk 2 (int 2) compound-scheme-data))) => anything)
       (fact "read cell with compound scheme"
             (time (do (read-cell @ttrunk 2))) => compound-scheme-data)
       (fact "delete cell"
             (time (do (delete-cell @ttrunk 2))) => anything)
       (fact "deleted cell cannot been read"
             (read-cell @ttrunk 2) => nil)
       (fact "replace cell"
             (time (do (replace-cell @ttrunk 1 simple-scheme-data-replacement))) => anything)
       (fact "cell should been replaced"
             (read-cell @ttrunk 1) => simple-scheme-data-replacement)
       (fact "shrinked replace cell"
             (new-cell @ttrunk 2 (int 2) compound-scheme-data) => anything
             (time (do (replace-cell @ttrunk 2 compound-scheme-data-shrinked-replacement))) => anything)
       (fact "shrinked cell should been replaced"
             (read-cell @ttrunk 2) => compound-scheme-data-shrinked-replacement)
       (fact "overflow replace cell"
             (time (do (replace-cell @ttrunk 2 compound-scheme-data-replacement))) => anything)
       (fact "overflow cell should been replaced"
             (read-cell @ttrunk 2) => compound-scheme-data-replacement)
       (fact "update cell"
             (time (do (update-cell @ttrunk 2 update :len-offset dec))) => (contains {:len-offset (dec (:len-offset compound-scheme-data-replacement))}))
       (fact "cell updated"
             (read-cell @ttrunk 2) => (contains {:len-offset (dec (:len-offset compound-scheme-data-replacement))}))
       (fact "dispose"
             (.dispose @ttrunk) => truthy))
