(ns neb.cell
  (:require [neb.types :refer [data-types]]
            [neb.schema :refer [schema-store]]
            [cluster-connector.utils.for-debug :refer [spy $]])
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

(defmacro gen-cell-header-offsets []
  (let [loc-counter (atom 0)]
    `(do ~@(map
             (fn [[prop type]]
               (let [{:keys [length]} (get data-types type)
                     out-code `(def ~(symbol (str (name prop) "-offset")) ~(deref loc-counter))]
                 (swap! loc-counter (partial + length))
                 out-code))
             cell-head-struc)
         ~(do (reset! loc-counter 0) nil)
         (def cell-head-struc-map
           ~(into {}
                  (map
                    (fn [[prop type]]
                      (let [{:keys [length] :as fields} (get data-types type)
                            res [prop (assoc (select-keys fields [:reader :writer]) :offset @loc-counter)]]
                        (swap! loc-counter (partial + length))
                        res))
                    cell-head-struc))))))

(gen-cell-header-offsets)

(defn schema-by-id [^Short schema-id]
  (-> (.getSchemaIdMap schema-store)
      (.get schema-id)))

(defn read-cell-header-field [^trunk trunk loc field]
  (let [{:keys [reader offset]} (get cell-head-struc-map field)]
    (reader trunk (+ loc offset))))

(defn write-cell-header-field [^trunk trunk loc field value]
  (let [{:keys [writer offset]} (get cell-head-struc-map field)]
    (writer trunk value (+ loc offset))))

(defn delete-cell [^trunk trunk ^Integer hash]
  (let [cell-loc (.get (.getCellIndex trunk) hash)
        length (read-cell-header-field trunk cell-loc :cell-length)]
    (.removeCellFromIndex trunk hash)
    (write-cell-header-field trunk cell-loc :cell-length (* -1 length))))

(defn read-cell [^trunk trunk ^Integer hash]
  (when-let [loc (.get (.getCellIndex trunk) hash)]
    (let [cell-reader (cellReader. trunk loc)]
      (with-cell
        cell-reader
        (when (> cell-length 0)
          (when-let [schema (schema-by-id schema-id)]
            (into
              {}
              (map
                (fn [[key-name data-type]]
                  [key-name
                   (let [{:keys [length reader dep dynamic? decoder unit-length]} (get data-types data-type)
                         dep (when dep (get data-types dep))
                         reader (or reader (get dep :reader))
                         reader (if decoder (comp decoder reader) reader)
                         length (or length
                                    (+ (* (reader/readInt
                                            trunk (.getCurrLoc cell-reader))
                                          unit-length)
                                       type_lengths/intLen))]
                     (.streamRead cell-reader reader length))])
                (:f schema)))))))))

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
                     (let [{:keys [length writer dep dynamic? encoder
                                   unit-length count-length]} (get data-types data-type)
                           dep (when dep (get data-types dep))
                           writer (or writer (get dep :writer))
                           field-data (get m key-name)
                           field-data (if encoder (encoder field-data) field-data)]
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
        (.streamWrite cell-writer writer value length))
      (.addCellToTrunkIndex cell-writer hash))))