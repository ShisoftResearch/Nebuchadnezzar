(ns neb.schema
  (:require [cluster-connector.utils.for-debug :refer [$ spy]])
  (:import (org.shisoft.neb schemaStore)))

(def ^schemaStore schema-store (schemaStore.))

(defn add-schema [sname fields & [id]]
  (let [id (short (or id (-> (.getIdCounter schema-store) (.getAndIncrement))))]
    (-> (.getSchemaIdMap schema-store)
        (.put id {:n sname :f fields :i id}))
    id))

(defn load-schemas-file [schema-file]
  (or (try (read-string (slurp schema-file)) (catch Exception _))
      []))

(defn load-schemas [schema-map]
  (doseq [{:keys [n f i]} schema-map]
    (add-schema n f i)))

(defn save-schemas [schema-file]
  (spit schema-file
        (pr-str (vec (-> (.getSchemaIdMap schema-store)
                         (.values))))
        :append false))