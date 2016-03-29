(ns neb.durability.serv.core
  (:require [neb.durability.serv.logs :as l]
            [neb.durability.serv.trunk :as t]
            [clojure.java.io :as io]
            [clojure.core.async :as a])
  (:import (org.shisoft.neb.durability.io BufferedRandomAccessFile)
           (java.util UUID)
           (java.util.concurrent.locks ReentrantLock)
           (java.io OutputStream File)))

(def start-time (System/nanoTime))
(def data-path (atom nil))
(def clients (atom {}))
(def ids (atom 0))
(def pending-logs (a/chan))

(declare collect-log)

(defn prepare-backup-server [path]
  (reset! data-path (str path "/" start-time "/"))
  (println "Starting backup server at:" @data-path)
  (a/go-loop [] (apply collect-log (a/<! pending-logs))))

(defn convert-server-name-for-file [^String str] (.replace str ":" "-"))

(defn file-base-path [server-name timestamp]
  (str @data-path "/"
       (convert-server-name-for-file server-name) "-"
       timestamp "-"))

(defn register-client-trunks [timestamp server-name trunks]
  (let [sid (swap! ids inc)
        base-path (let [^String path (file-base-path server-name timestamp)
                        file-ins (.getParentFile (File. path))]
                    (when-not (.exists file-ins) (.mkdirs file-ins))
                    path)
        replica-accessors (vec (map (fn [n] (let [file-path (str base-path n ".dat")
                                                  file-ins (File. file-path)]
                                              (when-not (.exists file-ins) (.createNewFile file-ins))
                                              (BufferedRandomAccessFile. file-path "rw")))
                                     (range trunks)))
        log-appenders (vec (map (fn [n] (atom (io/output-stream
                                                (str base-path n "-" timestamp ".mlog"))))
                                 (range trunks)))
        sync-timestamps (vec (map (fn [_] (atom 0)) (range trunks)))
        log-switches (vec (map (fn [_] (ReentrantLock.)) (range trunks)))]
    (swap! clients assoc sid
           {:server-name server-name
            :base-path   base-path
            :accessors   replica-accessors
            :appenders   log-appenders
            :sync-time   sync-timestamps
            :log-switches  log-switches})
    sid))

(defn sync-trunk [sid trunk-id loc ^bytes bs]
  (let [client (get @clients sid)
        accessor (get-in client [:accessors trunk-id])]
    (t/sync-to-disk accessor loc bs)))

(defn append-log [sid trunk-id act timestamp ^UUID cell-id & [data]]
  (let [client (get @clients sid)
        ^OutputStream appender @(get-in client [:appenders trunk-id])
        act (get {:write 0
                  :delete 1} act)]
    (a/go (a/>! pending-logs [appender act timestamp ^UUID cell-id data]))))

(defn switch-log [sid trunk-id timestamp]
  (let [client (get @clients sid)
        log-switch (get-in client [:log-switches trunk-id])
        base-path (:base-path client)]
    (.lock log-switch)
    (try
      (let [^OutputStream appender @(get-in client [:appenders trunk-id])
            ^OutputStream new-appender (io/output-stream (str base-path trunk-id "-" timestamp ".mlog"))]
        (reset! (get-in client [:appenders trunk-id]) new-appender)
        ;close orignal appender
        (a/go (a/>! pending-logs [appender -10 timestamp nil])))
      (finally
        (.unlock log-switch)))))

(defn finish-trunk-sync [sid trunk-id tail-loc timestamp]
  (let [client (get @clients sid)
        accessor (get-in client [:accessors trunk-id])]
    (.truncate (.getChannel accessor) tail-loc)
    (.flush accessor))
  (reset! (get-in (get @clients sid) [:sync-time trunk-id]) timestamp)
  (switch-log sid trunk-id timestamp))

(defn close-appender [appender]
  (.flush appender)
  (.close appender))

(defn collect-log [^OutputStream appender act timestamp ^UUID cell-id & [data]]
  (if (= -10 act)
    (close-appender appender)
    (l/append appender act timestamp cell-id data)))