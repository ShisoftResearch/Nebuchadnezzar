(ns neb.test.trunk
  (:require [midje.sweet :refer :all]
            [neb.schema :refer [add-scheme]]
            [neb.cell :refer [new-cell read-cell delete-cell]]
            [cluster-connector.utils.for-debug :refer [spy $]])
  (:import (org.shisoft.neb trunk schemaStore)
           (org.shisoft.neb.io cellReader cellWriter reader type_lengths)))

(def ttrunk (atom nil))
(def simple-scheme [[:int-value :int]])
(def simple-scheme-data {:int-value (rand-int Integer/MAX_VALUE)})

(def compound-scheme [[:int-value :int] [:long-value :long] [:char-value :char] [:text-value :text]
                      [:len-offset :int]])
(def compound-scheme-data {:int-value (rand-int Integer/MAX_VALUE) :long-value Long/MAX_VALUE
                           :char-value \测
                           :text-value "Google DeepMind开发的AlphaGo成为首个在公平比赛击败职业围棋选手的电脑围棋程式。 Нет никого, кто любил бы боль саму по себе, кто искал бы её и кто хотел бы иметь её просто потому, что это боль."
                           :len-offset 15})


(facts "trunk test"
       (fact "memory init"
             (reset! ttrunk (trunk. 5000)) => anything)
       (fact "define simple scheme"
             (add-scheme :test-schema simple-scheme 1) => anything)
       (fact "write cell with simple shceme"
             (time (do (new-cell @ttrunk (int 1) (short 1) simple-scheme-data) => anything)))
       (fact "read cell with simple scheme"
             (time (do (read-cell @ttrunk (int 1)) => simple-scheme-data)))
       (fact "define compound scheme"
             (time (do (add-scheme :test-schema2 compound-scheme 2) => anything)))
       (fact "write cell with compound shceme"
             (time (do (new-cell @ttrunk (int 2) (short 2) compound-scheme-data) => anything)))
       (fact "read cell with compound scheme"
             (time (do (read-cell @ttrunk (int 2)) => compound-scheme-data)))
       (fact "delete cell"
             (time (do (delete-cell @ttrunk (int 2)) => anything)))
       (fact "deleted cell cannot been read"
             (read-cell @ttrunk (int 2)) => nil)
       (fact "dispose"
             (.dispose @ttrunk) => truthy))
