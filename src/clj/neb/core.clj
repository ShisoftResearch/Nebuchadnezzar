(ns neb.core
  (:require [cluster-connector.remote-function-invocation.core :as rfi]
            [cluster-connector.distributed-store.core :refer [join-cluster with-cc-store leave-cluster] :as ds]
            [cluster-connector.sharding.core :refer [register-as-master checkout-as-master]]
            [cluster-connector.native-cache.core :refer :all]
            [cluster-connector.sharding.DHT :refer :all]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [cluster-connector.distributed-store.lock :as d-lock]
            [neb.schema :refer [load-schemas-file load-schemas clear-schemas schema-id-by-sname] :as s]
            [neb.trunk-store :refer [init-trunks dispose-trunks start-defrag stop-defrag]]
            [neb.utils :refer :all])
  (:import (java.util UUID)
           (com.google.common.hash Hashing MessageDigestHashFunction HashCode)
           (java.nio.charset Charset)))

(def cluster-config-fields [:trunks-size])
(def ^:dynamic *batch-size* 200)

(defn stop-server []
  (println "Shutdowning...")
  (rfi/stop-server)
  (stop-defrag)
  (dispose-trunks)
  (leave-cluster))

(defn start-server [config]
  (let [{:keys [server-name port zk meta]} config]
    (join-cluster
      :neb
      server-name
      port zk meta
      :connected-fn
      (fn []
        (let [cluster-configs (select-keys config cluster-config-fields)
              cluster-configs (or (try (:data (ds/get-configure :neb)) (catch Exception _))
                                  (do (ds/set-configure :neb cluster-configs)
                                      cluster-configs))
              {:keys [trunks-size]} cluster-configs
              {:keys [memory-size data-path]} config
              schemas (or (try (:data (ds/get-configure :schemas)) (catch Exception _))
                          (let [s (load-schemas-file (str data-path "/schemas"))]
                            (ds/set-configure :schemas s) s))
              trunk-count (int (Math/floor (/ memory-size trunks-size)))]
          (println "Loading Store...")
          (clear-schemas)
          (load-schemas schemas)
          (init-trunks trunk-count trunks-size)
          (start-defrag)
          (register-as-master (* 20 trunk-count))
          (rfi/start-server port)))
      :expired-fn
      (fn []
        (stop-server)))))

(defn clear-zk []
  (ds/delete-configure :schemas)
  (ds/delete-configure :neb))

(defn rand-cell-id [] (UUID/randomUUID))

(defn hash-str [string alog]
  (-> alog
      (.hashString string (Charset/forName "UTF-8"))
      (.asLong)))

(defn cell-id-by-key* [^String cell-key]
  (UUID.
    (hash-str cell-key (Hashing/sha1))
    (hash-str cell-key (Hashing/sha256))))

(defcache cell-id-by-key {:expire-after-access-secs :3600} cell-id-by-key*)

(defn locate-cell-by-id [^UUID cell-id]
  (get-server-for-name cell-id :hashing #(.getMostSignificantBits %)))

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

(defn new-cell* [id schema data]
  (dist-call
    id 'neb.trunk-store/new-cell
    (s/schema-id-by-sname schema)
    data))

(defn replace-cell* [id data]
  (dist-call id 'neb.trunk-store/replace-cell data))

(defn update-cell* [id fn & params]
  (apply dist-call id 'neb.trunk-store/update-cell fn params))

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

(d-lock/deflock schemas)

(defn add-schema [sname fields]
  (d-lock/locking
    schemas
    (let [server-new-ids (group-by identity (map second (rfi/broadcast-invoke 'neb.schema/gen-id)))
          new-id (apply max (keys server-new-ids))]
      (when (> (count server-new-ids) 1)
        (println "WARNING: Inconsistant schemas in server nodes. Synchronization required." (keys server-new-ids)))
      (rfi/broadcast-invoke 'neb.schema/add-schema sname fields new-id)
      new-id)))

(defn remove-schema [sname]
  (d-lock/locking
    schemas
    (let [schema-id (schema-id-by-sname sname)]
      (last (first (rfi/broadcast-invoke 'neb.schema/remove-schema-by-id schema-id))))))

(defn get-schemas []
  (.getSchemaIdMap s/schema-store))