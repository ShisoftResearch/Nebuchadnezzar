(ns neb.defragment
  (:require [neb.cell :refer [read-cell-header-field calc-trunk-cell-length cell-head-len]]
            [neb.schema :refer [schema-by-id]]
            [cluster-connector.utils.for-debug :refer [spy $]])
  (:import (org.shisoft.neb trunk)
           (org.shisoft.neb.io cellMeta)
           (java.util.concurrent ConcurrentSkipListMap)))

(defn unlock-frag [^trunk trunk]
  (.unlockFrags trunk))

(defmacro unlock-frag-recur [trunk pos]
  `(do (unlock-frag ~trunk)
       (recur ~pos)))

(defn scan-trunk-and-defragment [^trunk trunk]
  (let [frags (.getFragments trunk)]
    (loop [pos 0]
      (.lockFrags trunk)
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
              (let [cell-schema-id  (read-cell-header-field trunk hn-pos :schema-id)
                    cell-schema     (schema-by-id cell-schema-id)
                    cell-data-len   (calc-trunk-cell-length trunk hn-pos cell-schema)
                    cell-len        (+ cell-data-len cell-head-len)
                    cell-end-pos    (dec (+ hn-pos cell-len))
                    new-frag-pos    (+ lw-pos cell-len)]
                (.lockWrite cell-meta)
                (try
                  (.copyMemory trunk hn-pos lw-pos cell-len)
                  (.setLocation cell-meta lw-pos)
                  (finally
                    (.unlockWrite cell-meta)))
                (.removeFrag trunk lw-pos)
                (.addFragment_ trunk new-frag-pos cell-end-pos)
                (unlock-frag-recur trunk new-frag-pos))
              (= append-header (inc hi-pos))
              (do (.resetAppendHeader trunk pos)
                  (.removeFrag trunk pos)
                  (unlock-frag trunk))
              :else
              (do (spy [(.getFragments trunk) append-header])
                  (println "Missing cell meta when defrag at" hn-pos ", retry.")
                  (unlock-frag-recur trunk pos)))))))))