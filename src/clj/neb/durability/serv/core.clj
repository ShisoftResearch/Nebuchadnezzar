(ns neb.durability.serv.core
  (:require [neb.durability.serv.trunk :as t]
            [clojure.core.async :as a]
            [cluster-connector.utils.for-debug :refer [$ spy]])
  (:import (org.shisoft.neb.durability.io BufferedRandomAccessFile)
           (java.util.concurrent.locks ReentrantLock)
           (java.io File)
           (org.apache.commons.io FileUtils)
           (org.shisoft.neb Trunk)
           (com.google.common.primitives Ints)))

(def start-time (System/currentTimeMillis))
(def data-path (atom nil))
(def defed-path (atom nil))
(def clients (atom {}))
(def ids (atom 0))

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
  (println "Starting backup server at:" @data-path))

(defn convert-server-name-for-file [^String str] (.replace str ":" "-"))

(defn file-base-path [server-name timestamp]
  (str @data-path "/"
       (convert-server-name-for-file server-name) "-"
       timestamp "-"))

(defn prepare-replica [^BufferedRandomAccessFile accessor seg-size]
  (doto accessor
    (.seek 0)
    (.write (Ints/toByteArray seg-size))
    ))

(defn register-client-trunks [timestamp server-name trunks seg-size]
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
                                              (prepare-replica
                                                (BufferedRandomAccessFile. file-path "rw" (Trunk/getSegSize))
                                                seg-size)))
                                     (range trunks)))
        sync-timestamps (vec (map (fn [_] (atom 0)) (range trunks)))
        log-switches (vec (map (fn [_] (ReentrantLock.)) (range trunks)))
        pending-chan (a/chan 50)
        flushed-ch (atom nil)]
    (a/go-loop []
      (let [[act trunk-id seg-id base-addr current-addr bs] (a/<! pending-chan)
            accessor (replica-accessors trunk-id)]
        (try
          (case act
            0 (do (t/sync-seg-to-disk accessor seg-id seg-size base-addr current-addr bs))
            1 (do (.flush accessor)
                  (when @flushed-ch
                    (println "Flushed backup")
                    (a/>!! @flushed-ch true))))
          (catch Exception ex
            (clojure.stacktrace/print-cause-trace ex))))
      (recur))
    (swap! clients assoc sid
           {:server-name server-name
            :base-path   base-path
            :accessors   replica-accessors
            ;:appenders   log-appenders
            :sync-time   sync-timestamps
            :log-switches  log-switches
            :pending-chan pending-chan
            :flushed-ch flushed-ch})
    sid))

(defn plug-flushed-ch [chan]
  (doseq [[_ {:keys [flushed-ch]}] @clients]
    (reset! flushed-ch chan)))

(defn sync-trunk-segment [sid trunk-id seg-id base-addr currend-addr bs]
  (let [client (get @clients sid)]
    (a/>!! (:pending-chan client) [0 trunk-id seg-id base-addr currend-addr bs])))

(defn sync-trunk-completed [sid trunk-id]
  (let [client (get @clients sid)]
    (a/>!! (:pending-chan client) [1 trunk-id])))

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
  (println "Recovering Backups...")
  (let [dirs-to-recover (list-recover-dir)]
    (doseq [data-dir dirs-to-recover]
      (when (and (.isDirectory data-dir)
                 #_(not= (.getName (File. ^String @data-path)) (.getName data-dir)))
        (doseq [data-file (sort-by #(.getName %) (.listFiles data-dir))]
          (t/recover data-file))
        (.createNewFile (File. (str (.getAbsolutePath data-dir) "/imported"))))))
  (println "Backups distributed"))

(defn remove-imported-tag []
  (.delete (File. (str @data-path "/imported"))))

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