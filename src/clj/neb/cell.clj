(ns neb.cell
  (:require [neb.types :refer [data-types]]
            [neb.schema :refer [schema-store]])
  (:import (org.shisoft.neb trunk schemaStore)))

(def cell-struc [[:length :int] [:schema-id :short]])

(defmacro with-cell [^trunk trunk ^Integer loc & body]
  (let [loc-counter (atom 0)
        reader-symbol (symbol "reader")]
    `(let ~(vec (concat
                  (mapcat
                    (fn [[n t]]
                      [(symbol (name n))
                       `(let [~reader-symbol  (get-in data-types [~t :reader])]
                          ~(let [curr-loc @loc-counter]
                             (swap! loc-counter + (get-in data-types [ t :length]))
                             `(~reader-symbol ~trunk (+ ~loc ~curr-loc))))])
                    cell-struc)
                  [(symbol "data-loc") `(+ ~loc ~(deref loc-counter))]))
       ~@body)))

(defn read-cell [^trunk trunk ^Integer hash]
  (let [loc (.get (.getCellIndex trunk) hash)]
    (with-cell
      trunk loc
      (when-let [schema (-> (.getSchemaIdMap schema-store)
                            (.get schema-id))]
        (into
          {}
          (map
            (fn [[key-name data-type]]
              [key-name
               (let [{:keys [length reader dep dynamic?]} (get data-types data-type)]
                 )])
            (:f schema)))))))