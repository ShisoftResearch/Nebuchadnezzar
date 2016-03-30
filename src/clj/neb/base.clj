(ns neb.base
  (:require [cluster-connector.distributed-store.lock :as d-lock]))

(d-lock/deflock schemas-lock)