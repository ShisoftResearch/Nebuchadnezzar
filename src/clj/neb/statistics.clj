(ns neb.statistics
  (:require [neb.trunk-store :as ts]
            [cluster-connector.remote-function-invocation.core :as rfi])
  (:import (org.shisoft.neb Trunk Segment)))

(defn node-usage []
  (reduce + (mapcat (fn [^Trunk trunk]
                      (for [^Segment seg (.getSegments trunk)]
                        (.getAliveObjectBytes seg)))
                    (.getTrunks ts/trunks))))

(defn cluster-usage []
  (reduce + (map second (rfi/broadcast-invoke 'neb.statistics/node-usage))))
