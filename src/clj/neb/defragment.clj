(ns neb.defragment
  (:require [neb.header :refer [cell-head-len read-cell-header-field]]
            [neb.schema :refer [schema-by-id]]
            [cluster-connector.utils.for-debug :refer [spy $]])
  (:import (org.shisoft.neb Trunk)))

(set! *warn-on-reflection* true)

(defn scan-trunk-and-defragment [^Trunk ttrunk]
  (.defrag (.getCleaner ttrunk)))