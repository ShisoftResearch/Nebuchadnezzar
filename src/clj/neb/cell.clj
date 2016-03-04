(ns neb.cell
  (:require [neb.types :refer [data-types int-writer]]
            [neb.schema :refer [schema-store schema-by-id schema-id-by-sname walk-schema schema-by-sname]]
            [cluster-connector.utils.for-debug :refer [spy $]])
  (:import (org.shisoft.neb trunk schemaStore)
           (org.shisoft.neb.io cellReader cellWriter reader type_lengths cellMeta)))

(def ^:dynamic ^cellMeta *cell-meta* nil)
(def ^:dynamic *cell-hash* nil)

(def cell-head-struc
  [[:hash :long :hash]
   [:schema-id :int :schema]
   [:cell-length :int :length]])

(def cell-head-len
  (reduce + (map
              (fn [[_ type]]
                (get-in @data-types [type :length]))
              cell-head-struc)))

(defmacro with-cell [^cellReader cell-reader & body]
  `(let ~(vec (mapcat
                (fn [[n t]]
                  [(symbol (name n))
                   `(.streamRead
                      ~cell-reader
                      (get-in @data-types [~t :reader])
                      ~(get-in @data-types [ t :length]))])
                cell-head-struc))
     ~@body))

(defmacro gen-cell-header-offsets []
  (let [loc-counter (atom 0)]
    `(do ~@(map
             (fn [[prop type]]
               (let [{:keys [length]} (get @data-types type)
                     out-code `(def ~(symbol (str (name prop) "-offset")) ~(deref loc-counter))]
                 (swap! loc-counter (partial + length))
                 out-code))
             cell-head-struc)
         ~(do (reset! loc-counter 0) nil)
         (def cell-head-struc-map
           ~(into {}
                  (map
                    (fn [[prop type]]
                      (let [{:keys [length] :as fields} (get @data-types type)
                            res [prop (assoc (select-keys fields [:reader :writer]) :offset @loc-counter)]]
                        (swap! loc-counter (partial + length))
                        res))
                    cell-head-struc))))))

(defmacro with-cell-meta [trunk hash & body]
  `(with-bindings {#'*cell-meta* (-> ~trunk (.getCellIndex) (.get ~hash))
                   #'*cell-hash* ~hash}
     (when *cell-meta*
       ~@body)))

(defmacro with-write-lock [trunk hash & body]
  `(with-cell-meta
     ~trunk ~hash
     (locking *cell-meta*
       ~@body)))

(defmacro with-read-lock [trunk hash & body]
  `(with-cell-meta
     ~trunk ~hash
     (locking *cell-meta*
       ~@body)))

(defn get-cell-id []
  (.getLocation *cell-meta*))

(gen-cell-header-offsets)

(defn read-cell-header-field [^trunk trunk loc field]
  (let [{:keys [reader offset]} (get cell-head-struc-map field)]
    (reader trunk (+ loc offset))))

(defn write-cell-header-field [^trunk trunk loc field value]
  (let [{:keys [writer offset]} (get cell-head-struc-map field)]
    (writer trunk value (+ loc offset))))

(defn add-frag [^trunk trunk start end]
  (future     ;Use future to avoid deadlock with defragmentation daemon
    (locking (.getFragments trunk)
      (.addFragment trunk start end))))

(defn mark-cell-deleted [trunk cell-loc data-length]
  (add-frag trunk cell-loc (dec (+ cell-loc cell-head-len data-length))))

(defn calc-dynamic-type-length [trunk unit-length field-loc]
  (+ (* (reader/readInt trunk field-loc)
        unit-length)
     type_lengths/intLen))

;[[:id             :int]
; [:name           :text]
; [:map            [[:field1 :int] [:field2 :int]]]
; [:int-array     [:ARRRAY :int]]
; [:map-array      [:ARRAY [[:map-field :text]]]
; [:nested-array   [:ARRAY [:ARRAY :int]]]]

(def check-is-nested vector?)

(defn walk-schema-for-read [schema-fields ^trunk trunk ^cellReader cell-reader field-func map-func array-func]
  (let [recur-nested (fn [nested-schema & _]
                       (walk-schema-for-read
                         nested-schema trunk cell-reader
                         field-func map-func array-func))]
    (walk-schema
      schema-fields
      map-func
      (fn [field-name field-format]
        (let [type-props (get @data-types field-format)]
          (if type-props
            (let [{:keys [unit-length length]} type-props
                  field-length (or length (calc-dynamic-type-length trunk unit-length (.getCurrLoc cell-reader)))
                  field-result (field-func field-name (.getCurrLoc cell-reader) type-props field-length)]
              (.advancePointer cell-reader field-length)
              field-result)
            (recur-nested (:f (schema-by-sname field-format))))))
      (fn [field-name array-format]
        (let [array-len (reader/readInt trunk (.getCurrLoc cell-reader))
              nested-format? (check-is-nested array-format)
              array-format? (and nested-format? (= :ARRAY (first array-format)))
              type-format? (and (keyword array-format) (get @data-types array-format))
              nested-map-format? (and nested-format? (not array-format?))
              require-packing? (or array-format? type-format?)
              nested-schema (when (and (not nested-map-format?) (not require-packing?)) (:f (schema-by-sname array-format)))
              repeat-func (cond
                            nested-map-format?
                            #(recur-nested array-format)
                            nested-schema
                            #(recur-nested nested-schema)
                            require-packing?
                            #(:d (recur-nested [[:d array-format]])))]
          (.advancePointer cell-reader type_lengths/intLen)
          (apply
            array-func
            (doall (repeatedly array-len repeat-func))))))))

(defn walk-schema-for-write
  "It was assumed to have some side effect"
  [schema-fields data field-func map-func array-func array-header-func]
  (apply
    map-func
    (doall
      (map
        (fn [[field-name field-format]]
          [field-name
           (let [is-type? keyword?
                 is-nested? vector?
                 recur-nested (fn [nested-schema data & _]
                                (walk-schema-for-write
                                  nested-schema data field-func map-func
                                  array-func array-header-func))]
             (cond
               (is-nested? field-format)
               (if (= :ARRAY (first field-format))
                 (let [array-name field-name
                       array-format (second field-format)
                       array-items (get data array-name)
                       array-length (count array-items)
                       array-header (array-header-func array-length)
                       nested-format? (check-is-nested array-format)
                       array-format? (and nested-format? (= :ARRAY (first array-format)))
                       type-format? (and (keyword array-format) (get @data-types array-format))
                       nested-map-format? (and nested-format? (not array-format?))
                       require-packing? (or array-format? type-format?)
                       nested-schema (when (and (not nested-map-format?) (not require-packing?)) (:f (schema-by-sname array-format)))
                       map-func (cond
                                  nested-map-format?
                                  (partial recur-nested array-format)
                                  nested-schema
                                  (partial recur-nested nested-schema)
                                  require-packing?
                                  #(recur-nested [[:d array-format]] {:d %}))
                       array-content
                       (doall (map map-func array-items))]
                   (apply array-func array-name array-format array-header array-content))
                 (recur-nested field-format (get data field-name)))
               (is-type? field-format)
               (if (get @data-types field-format)
                 (field-func (get data field-name) field-name field-format (get @data-types field-format))
                 (recur-nested (:f (schema-by-sname field-format)) (get data field-name)))))])
        schema-fields))))

(defn plan-data-write [data schema]
  (->> (walk-schema-for-write
         (:f schema) data
         (fn [field-data field-name field-format field-props]
           (let [{:keys [length writer dep dynamic? encoder
                         unit-length count-array-length count-length]} field-props
                 dep (when dep (get @data-types dep))
                 writer (or writer (get dep :writer))
                 field-data (if encoder (encoder field-data) field-data)]
             (when (not (nil? field-data))
               {:value field-data
                :writer writer
                :length (if dynamic?
                          (cond
                            count-array-length
                            (+ (* (count-array-length field-data)
                                  unit-length)
                               type_lengths/intLen)
                            count-length
                            (count-length field-data))
                          length)})))
         (fn [& items]
           (map second items))
         (fn [_ _ array-header & array-content]
           [array-header array-content])
         (fn [len]
           {:value len
            :writer int-writer
            :length type_lengths/intLen}))
       (flatten)
       (filter identity)))

(defn read-cell* [^trunk trunk]
  (when-let [loc (get-cell-id)]
    (let [cell-reader (cellReader. trunk loc)]
      (with-cell
        cell-reader
        (when-let [schema (schema-by-id schema-id)]
          (merge (walk-schema-for-read
                   (:f schema) trunk cell-reader
                   (fn [field-name loc type-props field-length]
                     (let [{:keys [length reader dep dynamic? decoder unit-length]} type-props
                           dep (when dep (get @data-types dep))
                           reader (or reader (get dep :reader))
                           reader (if decoder (comp decoder reader) reader)]
                       (.streamRead cell-reader reader)))
                   (fn [& items]
                     (into {} items))
                   (fn [& items]
                     (into [] items)))
                 {:*schema* schema-id
                  :*hash*   *cell-hash*}))))))

(defn read-cell [^trunk trunk ^Long hash]
  (with-read-lock
    trunk hash
    (read-cell* trunk)))

(defn delete-cell [^trunk trunk ^Long hash]
  (with-write-lock
    trunk hash
    (if-let [cell-loc (get-cell-id)]
      (let [data-length (read-cell-header-field trunk cell-loc :cell-length)]
        (.removeCellFromIndex trunk hash)
        (mark-cell-deleted trunk cell-loc data-length))
      (throw (Exception. "Cell hash does not existed to delete")))))

(defmacro write-cell-header [cell-writer header-data]
  `(do ~@(map
           (fn [[head-name head-type head-data-func]]
             (let [{:keys [length]} (get @data-types head-type)]
               `(.streamWrite
                  ~cell-writer
                  (get-in @data-types [~head-type :writer])
                  (~head-data-func ~header-data)
                  ~length)))
           cell-head-struc)))

(defn cell-len-by-fields [fields-to-write]
  (reduce + (map :length fields-to-write)))

(defmacro locking-index [^trunk trunk & body]
  `(locking (.getCellIndex ~trunk)
     ~@body))

(defn write-cell [^trunk trunk ^Long hash schema data & {:keys [loc update-cell? update-hash-index?] :or {update-hash-index? true}}]
  (let [schema-id (:i schema)
        fields (plan-data-write data schema)
        fields-length (cell-len-by-fields fields)
        cell-length (+ cell-head-len fields-length)
        cell-writer (if loc
                      (cellWriter. ^trunk trunk ^Long cell-length loc)
                      (cellWriter. ^trunk trunk ^Long cell-length))
        header-data {:schema schema-id
                     :hash hash
                     :length fields-length}]
    (write-cell-header cell-writer header-data)
    (doseq [{:keys [value writer length] :as field} fields]
      (.streamWrite cell-writer writer value length))
    (when update-hash-index?
      (locking-index
        trunk
        (if update-cell?
          (.updateCellToTrunkIndex cell-writer hash)
          (.addCellToTrunkIndex cell-writer hash))))))

(defn new-cell [^trunk trunk ^Long hash ^Integer schema-id data]
  (when (.hasCell trunk hash)
    (throw (Exception. "Cell hash already exists")))
  (when-let [schema (schema-by-id schema-id)]
    (write-cell trunk hash schema data)))

(defn replace-cell* [^trunk trunk ^Long hash data]
  (when-let [cell-loc (get-cell-id)]
    (let [cell-data-loc (+ cell-loc cell-head-len)
          schema-id (read-cell-header-field trunk cell-loc :schema-id)
          schema (schema-by-id schema-id)
          data-len (read-cell-header-field trunk cell-loc :cell-length)
          fields (plan-data-write data schema)
          new-data-length (cell-len-by-fields fields)]
      (if (>= data-len new-data-length)
        (do (write-cell trunk hash schema data :loc cell-loc :update-hash-index? false)
            (when (< new-data-length data-len)
              (add-frag
                trunk
                (+ cell-data-loc new-data-length 1)
                (+ cell-data-loc data-len))))
        (do (write-cell trunk hash schema data :update-cell? true)
            (mark-cell-deleted trunk cell-loc data-len))))))

(defn replace-cell [^trunk trunk ^Long hash data]
  (with-write-lock
    trunk hash
    (replace-cell* trunk hash data)))

(defn update-cell [^trunk trunk ^Long hash fn & params]  ;TODO Replace with less overhead function
  (with-write-lock
    trunk hash
    (when-let [cell-content (read-cell* trunk)]
      (let [replacement  (apply fn cell-content params)]
        (replace-cell* trunk hash replacement)
        replacement))))