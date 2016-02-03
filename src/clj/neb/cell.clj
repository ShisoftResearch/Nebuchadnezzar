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

(defn mark-cell-deleted [trunk cell-loc data-length]
  (write-cell-header-field trunk cell-loc :cell-length (* -1 data-length)))

(defn delete-cell [^trunk trunk ^Integer hash]
  (if-let [cell-loc (.get (.getCellIndex trunk) hash)]
    (let [data-length (when cell-loc (read-cell-header-field trunk cell-loc :cell-length))]
      (.removeCellFromIndex trunk hash)
      (mark-cell-deleted trunk cell-loc data-length))
    (throw (Exception. "Cell hash does not existed to delete"))))

(defn read-cell [^trunk trunk ^Integer hash]
  (if-let [loc (.get (.getCellIndex trunk) hash)]
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

(defn cell-fields-to-write [schema data]
  (map
    (fn [[key-name data-type]]
      (let [{:keys [length writer dep dynamic? encoder
                    unit-length count-length]} (get data-types data-type)
            dep (when dep (get data-types dep))
            writer (or writer (get dep :writer))
            field-data (get data key-name)
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
    (:f schema)))

(defn cell-len-by-fields [fields-to-write]
  (reduce + (map :length fields-to-write)))

(defn write-cell [^trunk trunk schema data & {:keys [loc hash update-cell?]}]
  (let [schema-id (:i schema)
        fields (cell-fields-to-write schema data)
        fields-length (cell-len-by-fields fields)
        cell-length (+ cell-head-len fields-length)
        cell-writer (if loc
                      (cellWriter. ^trunk trunk ^Integer cell-length loc)
                      (cellWriter. ^trunk trunk ^Integer cell-length))
        header-data {:length fields-length
                     :schema schema-id}]
    (write-cell-header trunk cell-writer header-data)
    (doseq [{:keys [key-name type value writer length] :as field} fields]
      (.streamWrite cell-writer writer value length))
    (when hash
      (if update-cell?
        (.updateCellToTrunkIndex cell-writer hash)
        (.addCellToTrunkIndex cell-writer hash)))))

(defn new-cell [^trunk trunk ^Integer hash ^Integer schema-id data]
  (when (.hasCell trunk hash)
    (throw (Exception. "Cell hash already exists")))
  (when-let [schema (schema-by-id schema-id)]
    (write-cell trunk schema data :hash hash)))

(defn replace-cell [^trunk trunk ^Integer hash data]
  (when-let [cell-loc (.get (.getCellIndex trunk) hash)]
    (let [cell-data-loc (+ cell-loc cell-head-len)
          data-len (read-cell-header-field trunk cell-loc :cell-length)
          schema-id (read-cell-header-field trunk cell-loc :schema-id)
          schema (schema-by-id schema-id)
          fields (cell-fields-to-write schema data)
          new-data-length (cell-len-by-fields fields)]
      (if (>= data-len new-data-length)
        (do (write-cell trunk schema data :loc cell-loc)
            (when (< new-data-length data-len)
              (.addFragment
                trunk
                (+ cell-data-loc new-data-length)
                (+ cell-data-loc data-len))))
        (do (write-cell trunk schema data :hash hash :update-cell? true)
            (mark-cell-deleted trunk cell-loc data-len))))))

(defn update-cell [^trunk trunk ^Integer hash fn & params]  ;TODO Replace with less overhead function
  (when-let [cell-content (read-cell trunk hash)]
    (let [replacement  (apply fn cell-content params)]
      (replace-cell trunk hash replacement)
      replacement)))