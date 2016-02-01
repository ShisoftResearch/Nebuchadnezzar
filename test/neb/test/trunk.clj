(ns neb.test.trunk
  (:require [midje.sweet :refer :all]
            [neb.schema :refer [add-scheme]]
            [neb.cell :refer [write-cell read-cell]]
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
                           :text-value "Нет никого, кто любил бы боль саму по себе, кто искал бы её и кто хотел бы иметь её просто потому, что это боль."
                           :len-offset 15})


(facts "trunk test"
       (fact "memory init"
             (reset! ttrunk (trunk. 5000)) => anything)
       (fact "define simple scheme"
             (add-scheme :test-schema simple-scheme 1) => anything)
       (fact "write cell with simple shceme"
             (write-cell @ttrunk (int 123456) (short 1) simple-scheme-data) => anything)
       (fact "read cell with simple scheme"
             (read-cell @ttrunk (int 123456)) => simple-scheme-data)
       (fact "define compound scheme"
             (add-scheme :test-schema2 compound-scheme 2) => anything)
       (fact "write cell with compound shceme"
             (write-cell @ttrunk (int 123456) (short 2) compound-scheme-data) => anything)
       (fact "read cell with compound scheme"
             (read-cell @ttrunk (int 123456)) => compound-scheme-data)
       (fact "dispose"
             (.dispose @ttrunk) => truthy))
