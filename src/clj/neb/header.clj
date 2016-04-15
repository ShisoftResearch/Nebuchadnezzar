(ns neb.header
  (:require [neb.types :refer [data-types]])
  (:import (org.shisoft.neb Trunk)))

(def cell-head-struct
  [[:cell-type :byte :type]
   [:cell-length :int :length]
   [:partition :long :partition]
   [:hash :long :hash]
   [:schema-id :int :schema]])

(assert (and (= :byte (second (first cell-head-struct)))
             (= :int (second (second cell-head-struct)))) "second prop in header must be an integer for tombstone")

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

(defn get-cell-head-len [] cell-head-len)

(defn read-cell-header-field [^Trunk trunk loc field]
  (let [{:keys [reader offset]} (get cell-head-struc-map field)]
    (reader (+ loc offset))))

(defn write-cell-header-field [^Trunk trunk loc field value]
  (let [{:keys [writer offset]} (get cell-head-struc-map field)]
    (writer value (+ loc offset))))

(defn get-header-field-offset-length [field]
  (let [{:keys [length offset]} (get cell-head-struc-map field)]
    [offset length]))

(defn read-cell-hash [^Trunk trunk loc]
  (read-cell-header-field trunk loc :hash))

(defn read-cell-length [^Trunk trunk loc]
  (read-cell-header-field trunk loc :cell-length))