(ns neb.core
  (:require [cluster-connector.remote-function-invocation.core :as rfi]
            [cluster-connector.native-cache.core :refer :all]
            [cluster-connector.sharding.DHT :refer :all]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [cluster-connector.distributed-store.lock :as d-lock]
            [neb.schema :as s]
            [neb.utils :refer :all]
            [neb.base :refer [schemas-lock]]
            [neb.statistics])
  (:import (java.util UUID)
           (com.google.common.hash Hashing MessageDigestHashFunction HashCode HashFunction)
           (java.nio.charset Charset)
           (org.shisoft.neb.exceptions SchemaAlreadyExistsException)))

(set! *warn-on-reflection* true)

(def ^:dynamic *batch-size* 200)

(defn rand-cell-id [] (UUID/randomUUID))

(defn hash-str [string ^HashFunction alog]
  (-> alog
      (.hashString string (Charset/forName "UTF-8"))
      (.asLong)))

(defn cell-id-by-key* [^String cell-key]
  (UUID.
    (hash-str cell-key (Hashing/sha1))
    (hash-str cell-key (Hashing/sha256))))

(defcache cell-id-by-key {:expire-after-access-secs 60
                          :soft-values? true} cell-id-by-key*)

(defn locate-cell-by-id [^UUID cell-id]
  (get-server-for-name cell-id :hashing (fn [^UUID id] (.getMostSignificantBits id))))

(defn to-id [key]
  (if (= (class key) UUID)
    key
    (cell-id-by-key (name key))))

(defn- dist-call [cell-id func & params]
  (let [server-name (locate-cell-by-id cell-id)]
    (apply rfi/invoke server-name func cell-id params)))

(defn delete-cell* [id]
  (dist-call id 'neb.trunk-store/delete-cell))

(defn read-cell* [id]
  (dist-call id 'neb.trunk-store/read-cell))

(defn new-cell-by-ids [id schema-id data]
  (dist-call id 'neb.trunk-store/new-cell schema-id data)
  id)

(defn new-cell* [id schema data]
  (new-cell-by-ids id (s/schema-id-by-sname schema) data))

(defn replace-cell* [id data]
  (dist-call id 'neb.trunk-store/replace-cell data))

(defn update-cell* [id fn & params]
  (apply dist-call id 'neb.trunk-store/update-cell fn params))

(defn select-keys-from-cell* [id ks]
  (dist-call id 'neb.trunk-store/select-keys-from-cell ks))

(defn get-in-cell* [id ks]
  (dist-call id 'neb.trunk-store/get-in-cell ks))

(defn write-lock-exec* [id func-sym & params]
  (apply dist-call id 'neb.trunk-store/write-lock-exec func-sym params))

(defn new-cell-by-raw* [id bs]
  (dist-call id 'neb.trunk-store/new-cell-by-raw bs))

(defn cell-exists?* [id]
  (dist-call id 'neb.trunk-store/cell-exists?))

(defn read-cell-headers* [id]
  (dist-call id 'neb.trunk-store/read-cell-headers))

(defn get-batch-server-name [params-coll]
  (group-by
    first
    (map (fn [params]
           [(locate-cell-by-id (first params)) params])
         params-coll)))

(defn vector-ids [ids]
  (map (fn [id] [id]) ids))

(defn pre-proc-batch-params [coll func-sym]
  (cond
    (= func-sym 'new-cell)      (map (fn [params] (update params 1 s/schema-id-by-sname)) coll)
    (or (= func-sym 'read-cell)
        (= func-sym 'delete-cell)) (vector-ids coll)
    :else
    coll))

(defn proc-batch-indexer [coll conv op op-func]
  (let [coll (pre-proc-batch-params coll op)
        id-key-map (into {} (map (fn [[key#]] [(conv key#) key#]) coll))
        key-id-map (into {} (map (fn [[id# key#]] [key# id#]) id-key-map))
        coll (map (fn [params#] (update (vec params#) 0 (fn [key#] (get key-id-map key#)))) coll)]
    (map-on-keys
      (fn [cell-id#] (get id-key-map cell-id#))
      (op-func coll))))

(defmacro op-fns [func]
  (let [base-func (symbol (str (name func) "*"))
        base-batch-func (symbol (str "batch-" (name func) "*"))
        base-batch-reply    (symbol (str "neb.trunk-store/batch-" (name func)))
        base-batch-noreply  (symbol (str "neb.trunk-store/batch-" (name func) "-noreply"))]
    `(do (defn ~(symbol (str (name func) "-by-key")) [key# & params#]
           (apply ~base-func
                  (cell-id-by-key key#)
                  params#))
         (defn ~func [key# & params#]
           (apply ~base-func
                  (to-id key#)
                  params#))
         (defn ~base-batch-func [noreply?# coll#]
           (let [op-func-sym# (if noreply?# (quote ~base-batch-noreply) (quote ~base-batch-reply))
                 parts# (partition *batch-size* coll#)]
             (reduce
               merge
               (apply
                 concat
                 (for [server-op-list# parts#]
                   (let [server-op-list# (get-batch-server-name server-op-list#)]
                     (pmap
                       (fn [[server# params#]]
                         (rfi/invoke server# op-func-sym# (map second params#)))
                       server-op-list#)))))))
         (defn ~(symbol (str "batch-" (name func) "-by-key")) [coll#]
           (proc-batch-indexer coll# cell-id-by-key (quote ~func) (partial ~base-batch-func false)))
         (defn ~(symbol (str "batch-" (name func))) [coll#]
           (proc-batch-indexer coll# to-id (quote ~func) (partial ~base-batch-func false)))
         (defn ~(symbol (str "batch-" (name func) "-by-key-noreply")) [coll#]
           (proc-batch-indexer coll# cell-id-by-key (quote ~func) (partial ~base-batch-func true)))
         (defn ~(symbol (str "batch-" (name func) "-noreply")) [coll#]
           (proc-batch-indexer coll# to-id (quote ~func) (partial ~base-batch-func true))))))

(op-fns delete-cell)
(op-fns read-cell)
(op-fns new-cell)
(op-fns replace-cell)
(op-fns update-cell)
(op-fns get-in-cell)
(op-fns select-keys-from-cell)
(op-fns write-lock-exec)
(op-fns new-cell-by-raw)
(op-fns cell-exists?)
(op-fns read-cell-headers)

(defn add-schema [sname fields]
  (d-lock/locking
    schemas-lock
    (if (neb.schema/schema-sname-exists? sname)
      (throw (SchemaAlreadyExistsException. (name sname)))
      (rfi/condinated-invoke-with-selection
        ['neb.schema/gen-id nil]
        ['neb.schema/add-schema [sname fields '<>]] max))))

(defn remove-schema [sname]
  (d-lock/locking
    schemas-lock
    (let [schema-id (s/schema-id-by-sname sname)]
      (last (first (rfi/broadcast-invoke 'neb.schema/remove-schema-by-id schema-id))))))

(defn get-schemas []
  (.getSchemaIdMap s/schema-store))

(defn get-schema-by-name [sname]
  (s/schema-by-sname sname))

(defn get-schema-by-id [^Integer id]
  (s/schema-by-id id))

(defn schema-id-by-sname [sname]
  (s/schema-id-by-sname sname))