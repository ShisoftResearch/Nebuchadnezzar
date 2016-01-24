(ns neb.cell
  (:require [neb.types :refer [data-types]]
            [neb.schema :refer [schema-store]])
  (:import (org.shisoft.neb trunk schemaStore)
           (org.shisoft.neb.io cellReader cellWriter reader type_lengths)))

(def cell-head-struc
  [[:cell-length :int :length]
   [:schema-id :short :schema]])
(def cell-head-len
  (reduce + (map
              (fn [[_ type]]
                (get-in data-types [type :length]))
              cell-head-struc)))

(defmacro with-cell [^cellReader cell-reader & body]
  `(let ~(vec (mapcat
                (fn [[n t]]
                  [(symbol (name n))
                   `(.streamRead
                      ~cell-reader
                      (get-in data-types [~t :reader])
                      ~(get-in data-types [ t :length]))])
                cell-head-struc))
     ~@body))

(defn schema-by-id [^Integer schema-id]
  (-> (.getSchemaIdMap schema-store)
      (.get schema-id)))

(defn read-cell [^trunk trunk ^Integer hash]
  (let [loc (.get (.getCellIndex trunk) hash)
        cell-reader (cellReader. trunk loc)]
    (with-cell
      cell-reader
      (when-let [schema (schema-by-id schema-id)]
        (into
          {}
          (map
            (fn [[key-name data-type]]
              [key-name
               (let [{:keys [length reader dep dynamic? succproc unit-length]} (get data-types data-type)
                     dep (when dep (get data-types dep))
                     reader (or reader (get dep :reader))
                     reader (if succproc (comp succproc reader) reader)
                     length (or length
                                (+ (* (reader/readInt
                                        trunk (.getCurrLoc cell-reader))
                                      unit-length)
                                   type_lengths/intLen))]
                 (.streamRead cell-reader reader length))])
            (:f schema)))))))

(defmacro write-cell-header [trunk cell-writer header-data]
  `(do ~@(map
           (fn [[head-name head-type head-data-func]]
             (let [{:keys [length]} (get data-types head-type)]
               `(.streamWrite
                  ~cell-writer
                  (get-in data-types [~head-type :writer])
                  (~head-data-func ~header-data)
                  ~length)))
           cell-head-struc)))

(defn write-cell [^trunk trunk ^Integer hash ^Integer schema-id m]
  (when-let [schema (schema-by-id schema-id)]
    (let [fields (map
                   (fn [[key-name data-type]]
                     (let [{:keys [length writer dep dynamic? preproc
                                   unit-length count-length]} (get data-types data-type)
                           dep (when dep (get data-types dep))
                           writer (or writer (get dep :reader))
                           writer (if preproc (comp reader preproc) reader)
                           field-data (get m key-name)]
                       {:key-name key-name
                        :type data-type
                        :value field-data
                        :writer writer
                        :length (if dynamic?
                                  (+ (* (count-length field-data)
                                        unit-length)
                                     type_lengths/intLen)
                                  length)}))
                   (:f schema))
          fields-length (reduce + (map :length fields))
          cell-length (+ cell-head-len fields-length)
          cell-writer (cellWriter. ^trunk trunk ^Integer cell-length)
          header-data {:length fields-length
                       :schema schema-id}]
      (write-cell-header trunk cell-writer header-data)
      (doseq [{:keys [key-name type value writer length] :as field} fields]
        (.streamWrite cell-writer writer value length)))))