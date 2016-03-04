(ns neb.defragment
  (:require [neb.cell :refer [read-cell-header-field cell-head-len]]
            [neb.schema :refer [schema-by-id]]
            [cluster-connector.utils.for-debug :refer [spy $]])
  (:import (org.shisoft.neb trunk)
           (org.shisoft.neb.io cellMeta)
           (java.util.concurrent ConcurrentSkipListMap)))

(defn scan-trunk-and-defragment [^trunk trunk]
  (let [frags (.getFragments trunk)]
    (locking frags
      (loop [pos 0]
        (let [frag (.ceilingEntry frags pos)
              append-header (.getAppendHeaderValue trunk)]
          (if frag
            (let [lw-pos (.getKey frag)
                  hi-pos (.getValue frag)
                  hn-pos (inc hi-pos)
                  cell-hash (read-cell-header-field trunk hn-pos :hash)
                  ^cellMeta cell-meta (-> trunk (.getCellIndex) (.get cell-hash))]
              (cond
                cell-meta
                (let [cell-data-len   (read-cell-header-field trunk hn-pos :cell-length)
                      cell-len        (+ cell-data-len cell-head-len)
                      cell-end-pos    (dec (+ hn-pos cell-len))
                      new-frag-pos    (+ lw-pos cell-len)]
                  (locking cell-meta
                    (.copyMemory trunk hn-pos lw-pos cell-len)
                    (.setLocation cell-meta lw-pos))
                  (.removeFrag trunk lw-pos)
                  (.addFragment trunk new-frag-pos cell-end-pos)
                  (recur new-frag-pos))
                (= append-header (inc hi-pos))
                (do (.resetAppendHeader trunk pos)
                    (.removeFrag trunk pos))))))))))