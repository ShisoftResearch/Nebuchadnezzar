(ns neb.trunk-store
  (:require [neb.cell :as cell])
  (:import (org.shisoft.neb.io trunkStore)
           (java.util UUID)))

(def trunks (trunkStore.))

(defn init-trunks [trunk-count trunks-size]
  (.init trunks trunk-count trunks-size))

(defn dispose-trunks []
  (.dispose trunks))

(defn trunks-cell-count []
  (.getTrunksCellCount trunks))

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