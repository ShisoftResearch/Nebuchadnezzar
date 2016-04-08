(ns neb.defragment
  (:require [neb.cell :refer [read-cell-header-field pending-frags]]
            [neb.header :refer [cell-head-len]]
            [neb.schema :refer [schema-by-id]]
            [cluster-connector.utils.for-debug :refer [spy $]])
  (:import (org.shisoft.neb Trunk)
           (org.shisoft.neb.io CellMeta)))

(set! *warn-on-reflection* true)

;TODO: Rewrite this in Java
(defn scan-trunk-and-defragment* [^Trunk ttrunk]
  (try
    (.readLock ttrunk)
    (let [frags (.getFragments ttrunk)]
      (loop [pos 0]
        (let [frag (.ceilingEntry frags pos)
              append-header (.getAppendHeaderValue ttrunk)]
          (when frag
            (let [lw-pos (.getKey frag)
                  hi-pos (.getValue frag)
                  hn-pos (inc hi-pos)
                  cell-hash (read-cell-header-field ttrunk hn-pos :hash)
                  ^CellMeta cell-meta (-> ttrunk (.getCellIndex) (.get cell-hash))]
              (cond
                cell-meta
                (let [^Long new-frag-pos
                      (let [cell-data-len   (read-cell-header-field ttrunk hn-pos :cell-length)
                            cell-len        (+ cell-data-len cell-head-len)
                            cell-end-pos    (dec (+ hn-pos cell-len))
                            new-frag-pos    (long (+ lw-pos cell-len))
                            location-confirmed (atom false)]
                        (locking cell-meta
                          (reset! location-confirmed (= hn-pos (.getLocation cell-meta)))
                          (when @location-confirmed
                            (.copyMemory ttrunk hn-pos lw-pos cell-len)
                            (.setLocation cell-meta lw-pos)))
                        (when @location-confirmed
                          (.removeFrag ttrunk lw-pos)
                          (.addFragment ttrunk new-frag-pos cell-end-pos)
                          new-frag-pos))]
                  (when new-frag-pos (recur new-frag-pos)))
                (and (<= append-header (inc hi-pos))
                     (>= append-header lw-pos))
                (do                                         ;(println "Hit tail" lw-pos hi-pos cell-hash)
                  (.resetAppendHeader ttrunk lw-pos)
                  (.removeFrag ttrunk lw-pos))
                :else
                (do                                         ;(println "Unknown Frag:" lw-pos hi-pos cell-hash)
                  (.removeFrag ttrunk lw-pos)
                  (recur pos))))))))
    (finally
      (.readUnLock ttrunk))))

(defn scan-trunk-and-defragment [^Trunk ttrunk]
  (let [frags (.getFragments ttrunk)]
    (while (not (empty? frags)) (scan-trunk-and-defragment* ttrunk))))