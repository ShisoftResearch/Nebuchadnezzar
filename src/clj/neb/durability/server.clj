(ns neb.durability.server
  (:require [clojure.java.io :as io]
            [clojure.core.async :as a])
  (:import (org.shisoft.neb.durability.io BufferedRandomAccessFile)
           (com.google.common.primitives Ints Longs)
           (java.util UUID)))

(def start-time (System/currentTimeMillis))
(def data-path (atom nil))
(def clients (atom {}))
(def ids (atom 0))
(def log-reaper (a/chan))

(declare append-log*)

(defn prepare-backup-server [data-path]
  (reset! data-path (str data-path "/" start-time "/"))
  (a/go-loop [] (apply append-log* (a/<! log-reaper))))

(defn convert-server-name-for-file [^String str] (.replace str ":" "-"))

(defn register-client-trunks [timestamp server-name trunks]
  (let [sid (swap! ids inc)
        base-path (str @data-path "/"
                       (convert-server-name-for-file server-name) "-"
                       timestamp "-")
        replica-accessors (vec (map (fn [n] (BufferedRandomAccessFile.
                                               (str base-path n ".dat") "rw"))
                                     (range trunks)))
        log-appenders (vec (map (fn [n] (io/output-stream
                                           (str base-path n ".mlog")))
                                 (range trunks)))
        sync-timestamps (vec (map (fn [_] (atom 0)) (range trunks)))]
    (swap! clients assoc sid
           {:server-name server-name
            :accessors replica-accessors
            :appenders log-appenders
            :base-path base-path
            :sync-time sync-timestamps})
    sid))

(defn sync-trunk [sid trunk-id loc ^bytes bs]
  (let [client (get @clients sid)
        accessor (get-in client [:accessors trunk-id])]
    (locking accessor
      (.seek accessor loc)
      (.write accessor bs))))

(defn set-sync-time [sid trunk-id timestamp]
  (reset! (get-in (get @clients sid) [:sync-time trunk-id]) timestamp))

(defn append-log* [sid trunk-id act timestamp ^UUID cell-id & [data]]
  (let [client (get @clients sid)
        appender (get-in client [:appenders trunk-id])]
    (locking appender
      (doto appender
        (.write (byte act))
        (.write (Longs/toByteArray timestamp))
        (.write (Longs/toByteArray (.getLeastSignificantBits cell-id)))
        (.write (Longs/toByteArray (.getMostSignificantBits cell-id))))
      (when data
        (doto appender
          (.write (Ints/toByteArray (count data)))
          (.write data))))))

(defn append-log [sid trunk-id act timestamp ^UUID cell-id & [data]]
  (a/go (a/>! log-reaper [sid trunk-id act timestamp ^UUID cell-id data])))