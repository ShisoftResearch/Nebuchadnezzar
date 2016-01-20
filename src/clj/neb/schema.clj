(ns neb.schema
  (:import (org.shisoft.neb schemaStore)))

(def ^schemaStore schema-store (schemaStore.))

(defn add-schema [sname fields & id]
  (let [^Integer id (or id (-> (.getIdCounter schema-store) (.getAndIncrement)))]
    (-> (.getSchemaIdMap schema-store)
        (.put id {:n sname :f fields}))))