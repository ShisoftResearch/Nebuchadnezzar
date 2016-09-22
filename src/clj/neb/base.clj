(ns neb.base
  (:require [cluster-connector.distributed-store.lock :as d-lock]
            [cluster-connector.sharding.DHT :refer :all]
            [cluster-connector.utils.for-debug :refer [$ spy]])
  (:import (java.util UUID)))

(d-lock/deflock schemas-lock)

(defn locate-cell-by-id [^UUID cell-id]
  (get-server-for-name cell-id :hashing (fn [^UUID id] (.getMostSignificantBits id))))