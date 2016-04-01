(ns neb.client
  (:require [cluster-connector.distributed-store.core :refer [observe-cluster]]))

(defn login-cluster [region server-group store-address]
  (observe-cluster region server-group store-address))