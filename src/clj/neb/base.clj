(ns neb.base
  (:require [neb.types :refer [data-types]]))

(def cell-head-struct
  [[:partition :long :partition]
   [:hash :long :hash]
   [:cell-length :int :length]
   [:schema-id :int :schema]])

(defmacro gen-cell-header-offsets []
  (let [loc-counter (atom 0)]
    `(do ~@(map
             (fn [[prop type]]
               (let [{:keys [length]} (get @data-types type)
                     out-code `(def ~(symbol (str (name prop) "-offset")) ~(deref loc-counter))]
                 (swap! loc-counter (partial + length))
                 out-code))
             cell-head-struct)
         ~(do (reset! loc-counter 0) nil)
         (def cell-head-struc-map
           ~(into {}
                  (map
                    (fn [[prop type]]
                      (let [{:keys [length] :as fields} (get @data-types type)
                            res [prop (assoc (select-keys fields [:reader :writer]) :offset @loc-counter)]]
                        (swap! loc-counter (partial + length))
                        res))
                    cell-head-struct))))))

(gen-cell-header-offsets)

(def cell-head-len
  (reduce + (map
              (fn [[_ type]]
                (get-in @data-types [type :length]))
              cell-head-struct)))
