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
  (let [reader (io/input-stream log-path)]
    (while (> (.available reader) 0)
      (let [act (byte (.read reader))
            has-body? (zero? act)
            timestamp (read-long reader)
            cell-id (UUID. (read-long reader) (read-long reader))
            body-read? (when has-body? (atom false))
            body-length (when has-body? (Ints/fromByteArray (read-bytes reader 4)))
            read-body (when has-body?
                        (fn []
                          (reset! body-read? true)
                          (read-bytes reader body-length)))]
        (process-fn act timestamp cell-id read-body)
        (if (and has-body? (not @body-read?)) (.skip reader body-length))))))