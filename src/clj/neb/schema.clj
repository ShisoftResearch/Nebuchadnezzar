(ns neb.schema
  (:require [cluster-connector.utils.for-debug :refer [$ spy]]
            [cluster-connector.distributed-store.lock :as d-lock])
  (:import (org.shisoft.neb schemaStore)))

(def ^schemaStore schema-store (schemaStore.))

(defn add-schema* [sname fields id]
  (.put schema-store id {:n sname :f fields :i id}))

(d-lock/deflock schemas)

(defn add-schema-distributed [sname fields]
  (d-lock/locking schemas
    ))

(defn load-schemas-file [schema-file]
  (or (try (read-string (slurp schema-file)) (catch Exception _))
      []))

(defn load-schemas [schema-map]
  (doseq [{:keys [n f i]} schema-map]
    (add-schema* n f i)))

(defn save-schemas [schema-file]
  (spit schema-file
        (pr-str (vec (-> (.getSchemaIdMap schema-store)
                         (.values))))
        :append false))