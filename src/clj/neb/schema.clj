(ns neb.schema
  (:require [cluster-connector.utils.for-debug :refer [$ spy]])
  (:import (org.shisoft.neb schemaStore)))

(def ^schemaStore schema-store (schemaStore.))

(defn add-scheme [sname fields & [id]]
  (let [id (short (or id (-> (.getIdCounter schema-store) (.getAndIncrement))))]
    (-> (.getSchemaIdMap schema-store)
        (.put id {:n sname :f fields}))
    id))