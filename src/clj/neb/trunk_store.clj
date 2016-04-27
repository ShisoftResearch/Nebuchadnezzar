(ns neb.trunk-store
  (:require [neb.cell :as cell]
            [neb.durability.core :refer :all]
            [neb.defragment :as defrag]
            [cluster-connector.utils.for-debug :refer [spy $]]
            [cluster-connector.microservice.circular :as ms]
            [cluster-connector.replication.core :as rep]
            [cluster-connector.remote-function-invocation.core :as rfi]
            [cluster-connector.distributed-store.core :as ds]
            [com.climate.claypoole :as  cp])
  (:import (org.shisoft.neb.io TrunkStore)
           (java.util UUID)
           (org.shisoft.neb Trunk)))

(set! *warn-on-reflection* true)

(def ^TrunkStore trunks (TrunkStore.))
(def defrag-service (atom nil))
(def backup-service (atom nil))
(def ^:dynamic *cell-id*)

(defn init-trunks [trunk-count trunks-size durability]
  (.init trunks trunk-count trunks-size durability))

(defn init-durability-client [replication]
  (println "Registering to durability servers:" replication)
  (let [backup-servers (rep/select-random replication)
        sids (doall
               (map (fn [sn]
                      [sn (rfi/invoke sn 'neb.durability.serv.core/register-client-trunks
                                      (System/currentTimeMillis) @ds/this-server-name (.getTrunkCount trunks)
                                      (Trunk/getSegSize))])
                    backup-servers))]
    (println "Wil replicate to:" sids)
    (reset! server-sids sids)))

(defn dispose-trunks []
  (.dispose trunks))

(defn trunks-cell-count []
  (.getTrunksCellCount trunks))

(def ncpu (cp/ncpus))

(defn defrag-store-trunks [stopped-atom]
  (let [trunks (.getTrunks trunks)
        pool (cp/threadpool (count trunks)
                            :name "Defragmentation")
        defrag-trunk (fn [^Trunk trunk]
                       (while (not @stopped-atom)
                         (try
                           (defrag/scan-trunk-and-defragment trunk)
                           (catch Throwable tr (clojure.stacktrace/print-cause-trace tr)))
                         (Thread/sleep 1000))
                       (cp/shutdown pool))]
    (doseq [^Trunk trunk trunks]
      (cp/future pool (defrag-trunk trunk)))
    stopped-atom))

(defn backup-trunks [stopped-atom]
  (println "Starting Backup...")
  (let [trunks (.getTrunks trunks)
        pool (cp/threadpool (count trunks)
                            :name "Backup")
        backup (fn [^Trunk trunk]
                 (while (not @stopped-atom)
                   (try
                     (sync-trunk trunk)
                     (catch Throwable tr (clojure.stacktrace/print-cause-trace tr)))
                   (Thread/sleep 1000))
                 (cp/shutdown pool))]
    (doseq [trunk trunks]
      (cp/future pool (backup trunk)))
    stopped-atom))

(defn start-defrag []
  (reset! defrag-service (defrag-store-trunks (atom false))))

(defn stop-defrag []
  (reset! @defrag-service true))

(defn start-backup []
  (reset! backup-service (backup-trunks (atom false))))

(defn stop-backup []
  (reset! @backup-service true))

(defn dispatch-trunk [^UUID cell-id func & params]
  (with-bindings {#'*cell-id* cell-id}
    (let [hash (.getLeastSignificantBits cell-id)
          trunk-id (mod (.getMostSignificantBits cell-id)
                        (.getTrunkCount trunks))
          trunk (.getTrunk trunks (int trunk-id))]
      (assert trunk "Cannot get trunk for dispatch")
      (apply func trunk hash params))))

(defn get-trunk-store-params []
  {:trunks-size (.getTrunkSize trunks)
   :trunk-count  (.getTrunkCount trunks)})

(defn delete-cell [^UUID cell-id]
  (dispatch-trunk cell-id cell/delete-cell))

(defn read-cell [^UUID cell-id]
  (dispatch-trunk cell-id cell/read-cell))

(defn new-cell [^UUID cell-id schema-id data]
  (dispatch-trunk cell-id cell/new-cell (.getMostSignificantBits cell-id) schema-id data))

(defn replace-cell [^UUID cell-id data]
  (dispatch-trunk cell-id cell/replace-cell data))

(defn update-cell [^UUID cell-id func-sym & params]
  (apply dispatch-trunk cell-id cell/update-cell func-sym params))

(defn get-in-cell [^UUID cell-id ks]
  (dispatch-trunk cell-id cell/get-in-cell ks))

(defn select-keys-from-cell [^UUID cell-id ks]
  (dispatch-trunk cell-id cell/select-keys-from-cell ks))

(defn write-lock-exec [^UUID cell-id func-sym & params]
  (apply dispatch-trunk cell-id cell/write-lock-exec func-sym params))

(defn read-lock-exec [^UUID cell-id func-sym & params]
  (apply dispatch-trunk cell-id cell/read-lock-exec func-sym params))

(defn new-cell-by-raw [^UUID cell-id bs]
  (dispatch-trunk cell-id cell/new-cell-by-raw bs))

(defn cell-exists? [^UUID cell-id]
  (dispatch-trunk cell-id cell/cell-exists?))

(defn read-cell-headers [^UUID cell-id]
  (dispatch-trunk cell-id cell/read-cell-headers))

(defmacro batch-fn [func]
  `(do (defn ~(symbol (str "batch-" (name func))) [coll#]
         (into
           {}
           (for [[^UUID cell-id# & params#] coll#]
             [cell-id# (apply ~func cell-id# params#)])))
       (defn ~(symbol (str "batch-" (name func) "-noreply")) [coll#]
         (doseq [[^UUID cell-id# & params#] coll#]
           [cell-id# (apply ~func cell-id# params#)]))))

(batch-fn delete-cell)
(batch-fn read-cell)
(batch-fn new-cell)
(batch-fn replace-cell)
(batch-fn update-cell)
(batch-fn get-in-cell)
(batch-fn select-keys-from-cell)
(batch-fn new-cell-by-raw)
(batch-fn cell-exists?)
(batch-fn read-cell-headers)
(batch-fn read-lock-exec)