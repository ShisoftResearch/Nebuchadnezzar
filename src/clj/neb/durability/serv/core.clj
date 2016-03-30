(ns neb.durability.serv.core
  (:require [neb.durability.serv.logs :as l]
            [neb.durability.serv.trunk :as t]
            [clojure.java.io :as io]
            [clojure.core.async :as a]
            [cluster-connector.utils.for-debug :refer [$ spy]])
  (:import (org.shisoft.neb.durability.io BufferedRandomAccessFile)
           (java.util UUID)
           (java.util.concurrent.locks ReentrantLock)
           (java.io OutputStream File)
           (org.apache.commons.io FileUtils)))

(def start-time (System/nanoTime))
(def data-path (atom nil))
(def defed-path (atom nil))
(def clients (atom {}))
(def ids (atom 0))
(def ^:deprecated pending-logs (a/chan))

(declare collect-log)

(defn remove-imported [^String current-path]
  (let [path-file (File. ^String @defed-path)
        dir-files (.listFiles path-file)
        current-file (File. current-path)]
    (doseq [fdir dir-files]
        (when (and (.exists (File. (str (.getAbsolutePath fdir) "/imported")))
                   (not (= current-file fdir)))
          (FileUtils/deleteDirectory fdir)))))

(defn prepare-backup-server [path keep-imported?]
  (reset! defed-path path)
  (reset! data-path (str path "/" start-time "/"))
  (when keep-imported? (remove-imported @data-path))
  (println "Starting backup server at:" @data-path)
  #_(a/go-loop [] (apply collect-log (a/<! pending-logs))))

(defn convert-server-name-for-file [^String str] (.replace str ":" "-"))

(defn file-base-path [server-name timestamp]
  (str @data-path "/"
       (convert-server-name-for-file server-name) "-"
       timestamp "-"))

(defn register-client-trunks [timestamp server-name trunks]
  (let [sid (swap! ids inc)
        base-path (let [^String path (file-base-path server-name timestamp)
                        file-ins (.getParentFile (File. path))
                        imported-tag (File. (str (.getAbsolutePath file-ins) "/imported"))]
                    (when-not (.exists file-ins) (.mkdirs file-ins))
                    (when (.exists imported-tag) (.delete imported-tag))
                    path)
        replica-accessors (vec (map (fn [n] (let [file-path (str base-path n ".dat")
                                                  file-ins (File. file-path)]
                                              (when-not (.exists file-ins) (.createNewFile file-ins))
                                              (BufferedRandomAccessFile. file-path "rw")))
                                     (range trunks)))
        ;log-appenders (vec (map (fn [n] (atom (io/output-stream
        ;                                        (str base-path n "-" timestamp ".mlog"))))
        ;                         (range trunks)))
        sync-timestamps (vec (map (fn [_] (atom 0)) (range trunks)))
        log-switches (vec (map (fn [_] (ReentrantLock.)) (range trunks)))
        pending-chan (a/chan)]
    (a/go-loop []
      (let [[act trunk-id loc ^bytes bs] (a/<! pending-chan)
            accessor (replica-accessors trunk-id)]
        (case act
          0 (t/sync-to-disk accessor loc bs)
          1 (do (.truncate (.getChannel accessor) loc)
                (.flush accessor))))
      (recur))
    (swap! clients assoc sid
           {:server-name server-name
            :base-path   base-path
            :accessors   replica-accessors
            ;:appenders   log-appenders
            :sync-time   sync-timestamps
            :log-switches  log-switches
            :pending-chan pending-chan})
    sid))

(defn sync-trunk [sid trunk-id loc bs]
  (let [client (get @clients sid)]
    (a/go (a/>! (:pending-chan client) [0 trunk-id loc bs]))))

(defn finish-trunk-sync [sid trunk-id tail-loc timestamp]
  (let [client (get @clients sid)]
    (a/go (a/>! (:pending-chan client) [1 trunk-id tail-loc])))
  (reset! (get-in (get @clients sid) [:sync-time trunk-id]) timestamp)
  #_(switch-log sid trunk-id timestamp))

(defn ^:deprecated  append-log [sid trunk-id act timestamp ^UUID cell-id & [data]]
  (let [client (get @clients sid)
        ^OutputStream appender @(get-in client [:appenders trunk-id])
        act (get {:write 0
                  :delete 1} act)]
    (a/go (a/>! pending-logs [appender act timestamp ^UUID cell-id data]))))

(defn ^:deprecated  switch-log [sid trunk-id timestamp]
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

(defn ^:deprecated  close-appender [appender]
  (.flush appender)
  (.close appender))

(defn ^:deprecated  collect-log [^OutputStream appender act timestamp ^UUID cell-id & [data]]
  (if (= -10 act)
    (close-appender appender)
    (l/append appender act timestamp cell-id data)))

(defn list-recover-dir []
  (let [path-file (File. ^String @defed-path)
        dir-files (.listFiles path-file)]
    (->> (filter
           (fn [fdir]
             (let [imported? (.exists (File. (str (.getAbsolutePath fdir) "/imported")))]
               (not imported?)))
           dir-files)
         (sort-by #(.getName %)))))

(defn recover-backup []
  (let [dirs-to-recover (list-recover-dir)]
    (doseq [data-dir dirs-to-recover]
      (when (.isDirectory data-dir)
        (doseq [data-file (sort-by #(.getName %) (.listFiles data-dir))]
          (t/recover data-file))
        (.createNewFile (File. (str (.getAbsolutePath data-dir) "/imported")))))))

(defn list-backup-ids []
  (let [dirs-to-recover (list-recover-dir)]
    (mapcat
      (fn [data-dir]
        (if (.isDirectory data-dir)
          (mapcat
            (fn [data-file]
              (t/list-ids data-file))
            (sort-by #(.getName %) (.listFiles data-dir)))
          []))
      dirs-to-recover)))