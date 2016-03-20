(ns neb.defragment
  (:require [neb.cell :refer [read-cell-header-field cell-head-len pending-frags]]
            [neb.schema :refer [schema-by-id]]
            [cluster-connector.utils.for-debug :refer [spy $]]
            [clojure.core.async :as a])
  (:import (org.shisoft.neb trunk)
           (org.shisoft.neb.io cellMeta)))

(set! *warn-on-reflection* true)

(defn collecting-frags []
  (a/go-loop []
    (let [[^trunk ttrunk start end] (a/<! pending-frags)]
      (locking (.getFragments ttrunk)
        (.addFragment ttrunk start end)))))

(defn scan-trunk-and-defragment [^trunk ttrunk]
  (let [frags (.getFragments ttrunk)]
    (locking frags
      (loop [pos 0]
        (let [frag (.ceilingEntry frags pos)
              append-header (.getAppendHeaderValue ttrunk)]
          (if frag
            (let [lw-pos (.getKey frag)
                  hi-pos (.getValue frag)
                  hn-pos (inc hi-pos)
                  cell-hash (read-cell-header-field ttrunk hn-pos :hash)
                  ^cellMeta cell-meta (-> ttrunk (.getCellIndex) (.get cell-hash))]
              (cond
                cell-meta
                (let [cell-data-len   (read-cell-header-field ttrunk hn-pos :cell-length)
                      cell-len        (+ cell-data-len cell-head-len)
                      cell-end-pos    (dec (+ hn-pos cell-len))
                      new-frag-pos    (long (+ lw-pos cell-len))]
                  (locking cell-meta
                    (.copyMemory ttrunk hn-pos lw-pos cell-len)
                    (.setLocation cell-meta lw-pos))
                  (.removeFrag ttrunk lw-pos)
                  (.addFragment ttrunk new-frag-pos cell-end-pos)
                  (recur new-frag-pos))
                (= append-header (inc hi-pos))
                (do (.resetAppendHeader ttrunk pos)
                    (.removeFrag ttrunk pos))))))))))