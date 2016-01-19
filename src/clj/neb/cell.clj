(ns neb.cell
  (:import (org.shisoft.neb trunk)))

(def cell-struc [[:len :int] [:type :short]])

(defmacro with-cell [^trunk trunk ^Integer loc]
  )

(defn read-cell [^trunk trunk ^Integer hash]
  (let [loc (.get (.getCellIndex trunk) hash)]
    ))