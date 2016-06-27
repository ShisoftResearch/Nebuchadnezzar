(ns neb.snapshot
  (:import (org.shisoft.neb Trunk)
           (java.util Map$Entry)
           (org.shisoft.neb.io CellMeta)
           (org.shisoft.neb.io type_lengths)))

(defn thaw [^Trunk trunk ^String file-path]
  (let [cell-index (.getCellIndex trunk)
        index-pair-size (* 2 type_lengths/longLen)
        index-record-size ()
        index-pairs (map
                      (fn [^Map$Entry entry]
                        (let [^Integer hash (.getKey entry)
                              ^CellMeta cell-meta (.getValue entry)
                              location (.getLocation cell-meta)]
                          ))
                      (.entrySet cell-index))]))