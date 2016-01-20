(ns neb.cell
  (:require [neb.types :refer [data-types]]
            [neb.schema :refer [schema-store]])
  (:import (org.shisoft.neb trunk schemaStore)
           (org.shisoft.neb.io cellReader)))

(def cell-struc [[:length :int] [:schema-id :short]])

(defmacro with-cell [^cellReader cell-reader & body]
  `(let ~(vec (mapcat
                (fn [[n t]]
                  [(symbol (name n))
                   `(.streamRead
                      ~cell-reader
                      (get-in data-types [~t :reader])
                      ~(get-in data-types [ t :length]))])
                cell-struc))
     ~@body))

(defn read-cell [^trunk trunk ^Integer hash]
  (let [loc (.get (.getCellIndex trunk) hash)
        reader (cellReader. trunk loc)]
    (with-cell
      reader
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