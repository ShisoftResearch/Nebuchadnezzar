(ns neb.header
  (:require [neb.types :refer [data-types]])
  (:import (org.shisoft.neb Trunk)))

(def cell-head-struct
  [[:cell-type :byte :type]
   [:partition :long :partition]
   [:hash :long :hash]
   [:cell-length :int :length]
   [:schema-id :int :schema]])

(assert (and (= :byte (second (first cell-head-struct)))
             (= :long (second (second cell-head-struct)))) "second prop in header must be an integer for tombstone")

(defmacro gen-cell-header-offsets []
  (let [loc-counter (atom 0)]
    `(do ~@(map
             (fn [[prop type]]
               (let [{:keys [length]} (get @data-types type)
                     out-code `(def ~(symbol (str (name prop) "-offset")) ~(deref loc-counter))]
                 (swap! loc-counter (partial + length))
                 out-code))
             cell-head-struct)
         ~(do (reset! loc-counter 0) nil)
         (def cell-head-struc-map
           ~(into {}
                  (map
                    (fn [[prop type]]
                      (let [{:keys [length] :as fields} (get @data-types type)
                            res [prop (assoc (select-keys fields [:reader :writer]) :offset @loc-counter)]]
                        (swap! loc-counter (partial + length))
                        res))
                    cell-head-struct))))))

(gen-cell-header-offsets)

(def cell-head-len
  (reduce + (map
              (fn [[_ type]]
                (get-in @data-types [type :length]))
              cell-head-struct)))

(defn read-cell-header-field [^Trunk trunk loc field]
  (let [{:keys [reader offset]} (get cell-head-struc-map field)]
    (reader trunk (+ loc offset))))

(defn write-cell-header-field [^Trunk trunk loc field value]
  (let [{:keys [writer offset]} (get cell-head-struc-map field)]
    (writer trunk value (+ loc offset))))

(defn get-header-field-offset-length [field]
  (let [{:keys [length offset]} (get cell-head-struc-map field)]
    [offset length]))