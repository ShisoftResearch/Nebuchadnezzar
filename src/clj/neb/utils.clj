(ns neb.utils)

(defn map-on-vals [f m]
  (into {} (for [[k v] m] [k (f v)])))

(defn map-on-keys [f m]
  (into {} (for [[k v] m] [(f k) v])))