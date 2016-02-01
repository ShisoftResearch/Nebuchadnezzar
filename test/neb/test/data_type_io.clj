(ns neb.test.data-type-io
  (:require [midje.sweet :refer :all]
            [neb.schema :refer [add-scheme]]
            [neb.cell :refer [write-cell read-cell]]
            [neb.types :refer [data-types]]
            [cluster-connector.utils.for-debug :refer [spy $]])
  (:import (org.shisoft.neb trunk schemaStore)
           (org.shisoft.neb.io cellReader cellWriter reader type_lengths)))

(def ttrunk (atom nil))

(defmacro gen-test-cases []
  `(do ~@(map
           (fn [[dname {:keys [example]}]]
             (when (seq example)
               `(do (reset! ttrunk (trunk. 5000))
                    (add-scheme :test-type-schama [[:test ~dname]] 1)
                    ~@(map
                        (fn [test-case]
                          `(fact ~(str (name dname) " - " (pr-str test-case))
                                 (write-cell @ttrunk (int 123456) (short 1) {:test ~test-case}) => anything))
                        example)
                    (.dispose @ttrunk))))
           data-types)))

(facts "Data types io integrity test"
       (gen-test-cases))
