(ns neb.durability
  (:require [neb.defragment :as defrag]
            [taoensso.nippy :as nippy]
            [clojure.java.io :as io])
  (:import (org.shisoft.neb Trunk MemoryFork)
           (org.shisoft.neb.durability BackStore)
           (org.shisoft.neb.io CellMeta)
           (java.io DataOutputStream)))

;TODO: Durability for Nebuchadnezzar is still a undetermined feature.
;      The ideal design is to provide multi-master replication backend. Right now, there will be no replication.
;      Each object will only have on copy on local hard drive.
;      I thought about using memory map file, but it is uncontrolable and is also impossible to impelement replication in the future

(set! *warn-on-reflection* true)

(def data-path (atom nil))

(defn enable-durability [path]
  (reset! data-path path))

(defn set-trunk-backstore [^Trunk trunk id]
  (.setBackStore trunk (str @data-path "-" id)))

(defn update-meta [^BackStore bs meta-coll]
  (with-open [w (io/output-stream (.getMetaPath bs))]
    (nippy/freeze-to-out! (DataOutputStream. w) meta-coll)))

(defn sync-trunk [^Trunk trunk]
  (.writeLock trunk)
  (try
    (defrag/scan-trunk-and-defragment trunk)
    (assert (empty? (.getFragments trunk)) "Defrag not succeed")
    (let [^BackStore bs (.getBackStore trunk)
          dirty-ranges  (.clone (.getDirtyRanges trunk))
          append-header (.getAppendHeaderValue trunk)
          cell-metas (doall (map (fn [[^Long hash ^CellMeta meta]]
                                   [hash (.getLocation meta)])
                                 (.getCellIndex trunk)))
          ^MemoryFork mf (.fork trunk)]
      (.writeUnlock trunk)
      (loop [pos 0]
        (let [d-range (.ceilingEntry dirty-ranges pos)]
          (if-not d-range
            (.resetTail bs append-header)
            (do (locking dirty-ranges
                  (let [start (.getKey d-range)
                        end (.getValue d-range)]
                    (.syncRange bs start end)))
                (recur (.getValue d-range))))))
      (.release mf)
      (update-meta bs cell-metas))
    (finally
      (.writeUnlock trunk))))