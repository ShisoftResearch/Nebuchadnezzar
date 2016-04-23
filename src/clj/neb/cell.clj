(ns neb.cell
  (:require [neb.types :refer [data-types int-writer]]
            [neb.schema :refer [schema-store schema-by-id schema-id-by-sname walk-schema schema-by-sname]]
            [neb.header :refer [cell-head-struct cell-head-struc-map cell-head-len
                                read-cell-header-field write-cell-header-field get-header-field-offset-length]]
            [neb.durability.serv.native :refer [read-int]]
            [cluster-connector.remote-function-invocation.core :refer [compiled-cache]]
            [cluster-connector.utils.for-debug :refer [spy $]]
            [clojure.core.async :as a]
            [neb.defragment :as defrag])
  (:import (org.shisoft.neb Trunk)
           (org.shisoft.neb.io CellReader CellWriter Reader type_lengths CellMeta Writer)
           (java.util UUID)))

(set! *warn-on-reflection* true)

(def ^:dynamic ^CellMeta *cell-meta* nil)
(def ^:dynamic *cell-hash* nil)
(def ^:dynamic ^Trunk *cell-trunk* nil)

(defmacro with-cell [^CellReader cell-reader & body]
  `(let ~(vec (concat [(symbol "cell-header-map")
                       (into {}
                             (map (fn [[n t]]
                                    [n `(. ~cell-reader
                                           streamRead
                                           (get-in @data-types [~t :reader])
                                           ~(get-in @data-types [ t :length]))])
                                  cell-head-struct))]
                      (mapcat
                        (fn [[n t]]
                          [(symbol (name n))
                           `(get ~(symbol "cell-header-map") ~n)])
                        cell-head-struct)))
     ~@body))

(defn extract-cell-meta [ttrunk hash]
  (.getCellMeta ^Trunk ttrunk ^long hash))

(defmacro with-cell-meta [trunk hash & body]
  `(with-bindings {#'*cell-meta* (extract-cell-meta ~trunk ~hash)
                   #'*cell-hash* ~hash
                   #'*cell-trunk* ~trunk}
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

(defn get-cell-location []
  (let [loc (.getLocation *cell-meta*)]
    (assert (>= loc (.getStoreAddress *cell-trunk*))) loc))

(defn add-frag [^Trunk ttrunk start end]
  (.addFragment ttrunk start end))

(defn mark-cell-deleted [trunk cell-loc data-length]
  (add-frag trunk cell-loc (dec (+ cell-loc cell-head-len data-length))))

(defn calc-dynamic-type-length [trunk unit-length field-loc]
  (+ (* (Reader/readInt field-loc)
        unit-length)
     type_lengths/intLen))

;[[:id             :int]
; [:name           :text]
; [:map            [[:field1 :int] [:field2 :int]]]
; [:int-array     [:ARRRAY :int]]
; [:map-array      [:ARRAY [[:map-field :text]]]
; [:nested-array   [:ARRAY [:ARRAY :int]]]]

(def check-is-nested vector?)

(defn walk-schema-for-read [schema-fields trunk ^CellReader cell-reader field-func map-func array-func]
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
        (let [array-len (Reader/readInt (.getCurrLoc cell-reader))
              nested-format? (check-is-nested array-format)
              array-format? (and nested-format? (= :ARRAY (first array-format)))
              type-format? (and (keyword array-format) (get @data-types array-format))
              nested-map-format? (and nested-format? (not array-format?))
              require-packing? (or array-format? type-format?)
              nested-schema (when (and (not nested-map-format?) (not require-packing?)) (:f (schema-by-sname array-format)))
              repeat-func (cond
                            nested-map-format?
                            (fn [] (recur-nested array-format))
                            nested-schema
                            (fn [] (recur-nested nested-schema))
                            require-packing?
                            (fn [] (:d (recur-nested [[:d array-format]]))))]
          (.advancePointer cell-reader type_lengths/intLen)
          (array-func (doall (repeatedly array-len repeat-func))))))))

(defn walk-schema-for-write
  "It was assumed to have some side effect"
  [schema-fields data field-func map-func array-func array-header-func]
  (let [is-type? keyword?
        is-nested? vector?
        recur-nested (fn [nested-schema data & _]
                       (walk-schema-for-write
                         nested-schema data field-func map-func
                         array-func array-header-func))]
    (map-func
      (doall
        (map
          (fn [pvec]
            (let [field-name (pvec 0)
                  field-format (pvec 1)]
              [field-name
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
                                    (fn [x] (recur-nested array-format x))
                                    nested-schema
                                    (fn [x] (recur-nested nested-schema x))
                                    require-packing?
                                    (fn [x] (recur-nested [[:d array-format]] {:d x})))
                         array-content
                         (doall (map map-func array-items))]
                     (array-func array-name array-format array-header array-content))
                   (recur-nested field-format (get data field-name)))
                 (is-type? field-format)
                 (if (get @data-types field-format)
                   (field-func (get data field-name) field-name field-format (get @data-types field-format))
                   (recur-nested (:f (schema-by-sname field-format)) (get data field-name))))]))
          schema-fields)))))

(defrecord WritePlan [value writer length])

(defn plan-data-write [data schema]
  (->> (walk-schema-for-write
         (:f schema) data
         (fn [field-data field-name field-format field-props]
           (let [{:keys [length writer dep dynamic? encoder
                         unit-length count-array-length count-length checker]} field-props
                 dep (when dep (get @data-types dep))
                 writer (or writer (get dep :writer))]
             (when (and checker (not (checker field-data)))
               (throw (IllegalArgumentException. (str "Data check failed for field: " field-name " "
                                                      "Expect: " (name field-format) " "
                                                      "Actually: " (class field-data) " value: " field-data " "
                                                      "data: " data))))
             (let [field-data (if encoder (encoder field-data) field-data)]
               (when (not (nil? field-data))
                 (WritePlan. field-data writer
                             (if dynamic?
                               (cond
                                 count-array-length
                                 (+ (* (count-array-length field-data)
                                       unit-length)
                                    type_lengths/intLen)
                                 count-length
                                 (count-length field-data))
                               length))))))
         (fn [items]
           (map second items))
         (fn [_ _ array-header array-content]
           [array-header array-content])
         (fn [len]
           (WritePlan. len int-writer type_lengths/intLen)))
       (flatten)
       (filter identity)
       (doall)))

(def internal-cell-fields [:*schema* :*hash* :*id* :*version*])

(defn read-cell** [^Trunk trunk schema-fields ^CellReader cell-reader schema-id]
  (merge (walk-schema-for-read
           schema-fields trunk cell-reader
           (fn [_ _ type-props _]
             (let [{:keys [reader dep decoder]} type-props
                   dep (when dep (get @data-types dep))
                   reader (or reader (get dep :reader))
                   reader (if decoder (comp decoder reader) reader)]
               (.streamRead cell-reader reader)))
           (fn [items]
             (into {} items))
           (fn [items]
             (into [] items)))
         {:*schema* schema-id
          :*hash*   *cell-hash*}))

(defn read-cell* [^Trunk trunk]
  (when-let [loc (get-cell-location)]
    (let [cell-reader (CellReader. trunk loc)]
      (with-cell
        cell-reader
        (when-let [schema (schema-by-id schema-id)]
          (-> (read-cell** trunk (:f schema) cell-reader schema-id)
              (assoc :*id* (UUID. partition hash)
                     :*version* version)))))))

(defn read-cell [^Trunk trunk ^Long hash]
  (with-read-lock
    trunk hash
    (read-cell* trunk)))

(defn read-cell-headers [^Trunk trunk ^Long hash]
  (with-read-lock
    trunk hash
    (when-let [loc (get-cell-location)]
      (let [cell-reader (CellReader. trunk loc)]
        (with-cell cell-reader cell-header-map)))))

(defn delete-cell [^Trunk ttrunk ^Long hash]
  (with-write-lock
    ttrunk hash
    (if-let [cell-loc (get-cell-location)]
      (let [data-length (read-cell-header-field ttrunk cell-loc :cell-length)]
        (mark-cell-deleted ttrunk cell-loc data-length)
        (.removeCellFromIndex ttrunk hash))
      (throw (Exception. "Cell hash does not existed to delete")))))

(defmacro write-cell-header [cell-writer header-data]
  `(do ~@(map
           (fn [coll-param]
             (let [head-type (coll-param 1)
                   head-data-func (coll-param 2)
                   {:keys [length]} (get @data-types head-type)]
               `(.streamWrite
                  ~cell-writer
                  (get-in @data-types [~head-type :writer])
                  (~head-data-func ~header-data)
                  ~length)))
           cell-head-struct)))

(defn cell-len-by-fields [fields-to-write]
  (reduce + (map :length fields-to-write)))

(defn mark-dirty [^CellWriter cell-writer]
  (.markDirty cell-writer))

(def normal-cell-type (byte 1))

(defn write-cell [^Trunk ttrunk ^Long hash ^Long partition schema data & {:keys [^Long loc update-hash-index?  planned-data version] :or {update-hash-index? true version 0}}]
  (let [schema-id (:i schema)
        fields (or planned-data (plan-data-write data schema))
        fields-length (cell-len-by-fields fields)
        ^Long cell-length (+ cell-head-len fields-length)
        header-data {:schema schema-id
                     :hash hash
                     :partition partition
                     :length fields-length
                     :type normal-cell-type
                     :version version}]
    (let [^CellWriter cell-writer (if loc
                                    (CellWriter. ttrunk cell-length loc)
                                    (CellWriter. ttrunk cell-length))]
      (try
        (write-cell-header cell-writer header-data)
        (doseq [{:keys [value writer length]} fields]
          (.streamWrite cell-writer writer value length))
        (if update-hash-index?
          (.updateCellToTrunkIndex cell-writer *cell-meta* ttrunk)
          (.addCellMetaToTrunkIndex cell-writer hash ttrunk))
        (mark-dirty cell-writer)
        (catch Throwable tr
          (.rollBack cell-writer)
          (clojure.stacktrace/print-cause-trace tr))))))

(defn new-cell-by-raw [^Trunk ttrunk ^Long hash ^bytes bs]
  (let [cell-length (count bs)
        cell-writer (CellWriter. ttrunk cell-length)
        bytes-writer (fn [value curr-loc] (Writer/writeRawBytes value curr-loc))]
    (.streamWrite cell-writer bytes-writer bs cell-length)
    (mark-dirty cell-writer)
    (.addCellMetaToTrunkIndex cell-writer hash ttrunk)))

(defn cell-exists? [^Trunk ttrunk ^Long hash]
  (.hasCell ttrunk hash))

(defn new-cell [^Trunk ttrunk ^Long hash ^Long partition ^Integer schema-id data]
  (when (cell-exists? ttrunk hash)
    (throw (Exception. "Cell hash already exists")))
  (when-let [schema (schema-by-id schema-id)]
    (write-cell ttrunk hash partition schema data :update-hash-index? false)))

(defn replace-cell* [^Trunk trunk ^Long hash data]
  (when-let [cell-loc (get-cell-location)]
    (let [schema-id (read-cell-header-field trunk cell-loc :schema-id)
          schema (schema-by-id schema-id)
          data-len (read-cell-header-field trunk cell-loc :cell-length)
          partition (read-cell-header-field trunk cell-loc :partition)
          version (read-cell-header-field trunk cell-loc :version)
          fields (plan-data-write data schema)
          new-data-length (cell-len-by-fields fields)]
      (assert (> data-len 0))
      (if (= data-len new-data-length)
        (write-cell trunk hash partition schema data :loc cell-loc :planned-data fields :version (inc version))
        (do (write-cell trunk hash partition schema data :planned-data fields :version (inc version))
            (mark-cell-deleted trunk cell-loc data-len)))
      data)))

(defn replace-cell [^Trunk trunk ^Long hash data]
  (with-write-lock
    trunk hash
    (replace-cell* trunk hash data)))

(defn update-cell [^Trunk trunk ^Long hash fn & params]  ;TODO Replace with less overhead function
  (with-write-lock
    trunk hash
    (when-let [cell-content (read-cell* trunk)]
      (let [replacement  (apply (compiled-cache fn) cell-content params)]
        (assert replacement)
        (if-not (= cell-content replacement)
          (replace-cell* trunk hash replacement)
          cell-content)))))

(defn- compile-schema-for-get-in* [schema-fields ks]
  (loop [commetted-fields (transient [])
         remain-fields schema-fields]
    (let [field-pair (first remain-fields)
          [field-name field-type] field-pair
          current-key (first ks)]
      (cond
        (empty? remain-fields)
        commetted-fields
        (= field-name current-key)
        (persistent!
          (conj! commetted-fields
                 (if (or (= 1 (count ks))
                         (keyword? field-type))
                   field-pair
                   [field-name (compile-schema-for-get-in* field-type (rest ks))])))
        :else
        (recur (conj! commetted-fields field-pair)
               (rest remain-fields))))))

(defn- compile-schema-for-get-in [schema-fields ks]
  (compile-schema-for-get-in*
    schema-fields
    (loop [committed (transient [])
           keys-remains ks]
      (let [curr-key (first keys-remains)]
        (if (or (number? curr-key)
                (empty? keys-remains))
          (persistent! committed)
          (recur (conj! committed curr-key)
                 (rest keys-remains)))))))

(defn get-in-cell [^Trunk trunk ^Long hash ks]
  (if (keyword? ks)
    (get-in-cell trunk hash [ks])
    (with-read-lock
      trunk hash
      (when-let [loc (get-cell-location)]
        (let [cell-reader (CellReader. trunk loc)]
          (with-cell
            cell-reader
            (when-let [schema (schema-by-id schema-id)]
              (let [compiled-schema (compile-schema-for-get-in (:f schema) ks)
                    partical-cell (read-cell** trunk compiled-schema cell-reader schema-id)]
                (get-in partical-cell ks)))))))))

(defn- get-last-key-schema-for-select [schema-fields ks]
  (loop [committed-fields (transient [])
         keys-remains  (set ks)
         fields-to-check schema-fields]
    (let [field-pair (first fields-to-check)
          [field-name _] field-pair]
      (if (or (and (= 1 (count keys-remains))
                   (= field-name (first keys-remains)))
              (= 1 (count fields-to-check)))
        (persistent! (conj! committed-fields field-pair))
        (recur (conj! committed-fields field-pair)
               (disj keys-remains field-name)
               (rest fields-to-check))))))

(defn select-keys-from-cell [^Trunk trunk ^Long hash ks]
  (with-read-lock
    trunk hash
    (when-let [loc (get-cell-location)]
      (let [cell-reader (CellReader. trunk loc)]
        (with-cell
          cell-reader
          (when-let [schema (schema-by-id schema-id)]
            (let [compiled-schema (get-last-key-schema-for-select (:f schema) ks)
                  partical-cell (read-cell** trunk compiled-schema cell-reader schema-id)]
              (select-keys partical-cell ks))))))))

(defn write-lock-exec [^Trunk trunk ^Long hash func-sym & params]
  (with-write-lock
    trunk hash
    (apply (compiled-cache func-sym) (read-cell trunk hash) params)))