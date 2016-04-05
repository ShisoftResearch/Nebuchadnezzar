(ns ^:deprecated  neb.durability.serv.logs
  (:require [neb.durability.serv.file-reader :refer [read-bytes]]
            [neb.core :refer [new-cell-by-raw* delete-cell* replace-cell*]]
            [clojure.java.io :as io])
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

(defn read-long [reader]
  (Longs/fromByteArray (read-bytes reader 8)))

(defn read-all-from-log [log-path process-fn]
  (let [reader (io/input-stream log-path)]
    (while (> (.available reader) 0)
      (let [act (byte (.read reader))
            has-body? (pos? act)
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

(defn recover [file-path trunk-timestamp]
  (read-all-from-log
    file-path
    (fn [act timestamp cell-id read-body]
      (when (> timestamp trunk-timestamp)
        (case act
          0 (new-cell-by-raw* cell-id (read-body))
          1 (replace-cell* cell-id (read-body))
          -1 (delete-cell* cell-id))))))