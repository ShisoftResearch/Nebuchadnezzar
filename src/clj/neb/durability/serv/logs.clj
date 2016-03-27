(ns neb.durability.serv.logs
  (:require [clojure.java.io :as io])
  (:import (com.google.common.primitives Ints Longs)
           (java.io OutputStream)
           (java.util UUID)))

(defn append [^OutputStream appender act timestamp ^UUID cell-id & [^bytes data]]
  (doto appender
    (.write (byte act))
    (.write (Longs/toByteArray timestamp))
    (.write (Longs/toByteArray (.getLeastSignificantBits cell-id)))
    (.write (Longs/toByteArray (.getMostSignificantBits cell-id))))
  (when data
    (doto appender
      (.write (Ints/toByteArray (count data)))
      (.write data))))

(defn read-all-from-log [log-path process-fn]
  (let [reader (io/input-stream log-path)]))