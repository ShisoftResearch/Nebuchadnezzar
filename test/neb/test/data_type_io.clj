(ns neb.test.data-type-io
  (:require [midje.sweet :refer :all]
            [neb.schema :refer [add-scheme]]
            [neb.cell :refer [new-cell read-cell schema-by-id]]
            [neb.types :refer [data-types]]
            [cluster-connector.utils.for-debug :refer [spy $]])
  (:import (org.shisoft.neb trunk schemaStore)
           (org.shisoft.neb.io cellReader cellWriter reader type_lengths)))

(def ttrunk (atom nil))

(defn rt []
  (reset! ttrunk (trunk. 5000)))

(defn dt []
  (.dispose @ttrunk))

(defn wc [test-case]
  (new-cell @ttrunk (int 123456) (int 20) test-case))

(defn rc []
  (read-cell  @ttrunk (int 123456)))

(defmacro gen-test-cases []
  `(do ~@(map
           (fn [[dname {:keys [example]}]]
             (when (seq example)
               `(do (add-scheme :test-type-schama [[:test ~dname]] (short 20))
                    (schema-by-id (int 20)) =not=> nil
                    ~@(map
                        (fn [test-case]
                          `(fact ~(str (name dname) " - " (pr-str test-case))
                                 (let [test-case# {:test ~test-case}]
                                   (rt)
                                   (wc test-case#) => anything
                                   (rc) => test-case#
                                   (dt))))
                        example))))
           data-types)))

(facts "Data types io integrity test"
       (gen-test-cases))
