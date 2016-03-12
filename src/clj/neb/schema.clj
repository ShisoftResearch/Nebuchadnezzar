(ns neb.schema
  (:require [cluster-connector.utils.for-debug :refer [$ spy]]
            [cluster-connector.distributed-store.lock :as d-lock])
  (:import (org.shisoft.neb schemaStore)))

(def ^schemaStore schema-store (schemaStore.))

(defn schema-by-id [^Integer schema-id]
  (-> (.getSchemaIdMap schema-store)
      (.get schema-id)))

(defn schema-id-by-sname [sname]
  (.sname2Id schema-store sname))

(defn schema-by-sname [sname]
  (schema-by-id (schema-id-by-sname sname)))

(defn schema-sname-exists? [sname]
  (.snameExists schema-store sname))

(defn add-schema [sname fields id]
  (.put schema-store id sname {:n sname :f fields :i id}))

(defn clear-schemas []
  (.clear schema-store))

(defn remove-schema [id sname]
  (.remove schema-store id sname) id)

(defn remove-schema-by-sname [sname]
  (let [id (schema-id-by-sname sname)]
    (remove-schema id sname)))

(defn remove-schema-by-id [id]
  (let [sname (:n (schema-by-id id))]
    (remove-schema id sname)))

(defn load-schemas-file [schema-file]
  (or (try (read-string (slurp schema-file)) (catch Exception _))
      []))

(defn load-schemas [schema-map]
  (doseq [{:keys [n f i]} schema-map]
    (add-schema n f i)))

(defn gen-id []
  (locking schema-store
    (let [existed-ids (sort (keys (.getSchemaIdMap schema-store)))
          ids-range   (range)]
      (loop [e-ids existed-ids
             r-ids ids-range]
        (if(not= (first e-ids) (first r-ids))
          (first r-ids)
          (recur (rest e-ids)
                 (rest r-ids)))))))

(defn get-schemas []
  (vec (-> (.getSchemaIdMap schema-store)
           (.values))))

(defn save-schemas [schema-file]
  (spit schema-file
        (pr-str (get-schemas))
        :append false))

(defn walk-schema [schema-fields map-func field-func array-func]
  (apply
    map-func
    (doall
      (map
        (fn [[field-name field-format]]
          [field-name
           (let [is-type? keyword?
                 is-nested? vector?]
             (cond
               (is-nested? field-format)
               (if (= :ARRAY (first field-format))
                 (array-func field-name (second field-format))
                 (walk-schema field-format map-func field-func array-func))
               (is-type? field-format)
               (field-func field-name field-format)))])
        schema-fields))))