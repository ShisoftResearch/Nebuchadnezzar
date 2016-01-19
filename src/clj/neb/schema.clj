(ns neb.schema
  (:import (org.shisoft.neb schemaStore)))

(def schema-store (schema-store.))

(defn add-schema [sname fields & ^Integer id]
  (let [^Integer id (or id (-> (.getIdCounter schema-store) (.getAndIncrement)))]
    (-> (.getSchemaIdMap schema-store)
        (.put id {:n sname :f fields}))))