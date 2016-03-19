(ns neb.utils)

(set! *warn-on-reflection* true)

(defn map-on-vals [f m]
  (into {} (for [[k v] m] [k (f v)])))

(defn map-on-keys [f m]
  (into {} (for [[k v] m] [(f k) v])))

(defmacro try-all [& body]
  `(do ~@(map
           (fn [line] `(try ~line (catch Exception _#)))
           body)))