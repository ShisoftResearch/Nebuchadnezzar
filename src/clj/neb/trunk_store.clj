(ns neb.trunk-store
  (:require [neb.cell :as cell]
            [neb.defragment :as defrag]
            [cluster-connector.utils.for-debug :refer [spy $]]
            [cluster-connector.microservice.circular :as ms])
  (:import (org.shisoft.neb.io trunkStore)
           (java.util UUID)))

(def trunks (trunkStore.))
(def defrag-service (atom nil))

(defn init-trunks [trunk-count trunks-size]
  (.init trunks trunk-count trunks-size))

(defn dispose-trunks []
  (.dispose trunks))

(defn trunks-cell-count []
  (.getTrunksCellCount trunks))

(defn defrag-store-trunks []
  (doseq [trunk (.getTrunks trunks)]
    (defrag/scan-trunk-and-defragment trunk))
  (Thread/sleep 1000))

(defn start-defrag []
  ($ reset! defrag-service (ms/start-service defrag-store-trunks)))

(defn stop-defrag []
  ($ ms/stop-service @defrag-service))

(defn dispatch-trunk [^UUID cell-id func & params]
  (let [hash (.getLeastSignificantBits cell-id)
        trunk-id (mod (.getMostSignificantBits cell-id)
                      (.getTrunkCount trunks))
        trunk (.getTrunk trunks (int trunk-id))]
    (apply func trunk hash params)))

(defn get-trunk-store-params []
  {:trunks-size (.getTrunkSize trunks)
   :trunk-count  (.getTrunkCount trunks)})

(defn delete-cell [^UUID cell-id]
  (dispatch-trunk cell-id cell/delete-cell))

(defn read-cell [^UUID cell-id]
  (dispatch-trunk cell-id cell/read-cell))

(defn new-cell [^UUID cell-id schema-id data]
  (dispatch-trunk cell-id cell/new-cell schema-id data))

(defn replace-cell [^UUID cell-id data]
  (dispatch-trunk cell-id cell/replace-cell data))

(defn update-cell [^UUID cell-id fn & params]
  (apply dispatch-trunk cell-id cell/update-cell fn params))

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