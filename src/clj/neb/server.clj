(ns neb.server
  (:require [cluster-connector.remote-function-invocation.core :as rfi]
            [cluster-connector.distributed-store.core :refer [delete-configure is-first-node?
                                                              join-cluster with-cc-store leave-cluster] :as ds]
            [cluster-connector.sharding.core :refer [register-as-master checkout-as-master]]
            [neb.schema :as s]
            [neb.durability.serv.core :as dserv]
            [neb.utils :refer :all]
            [neb.trunk-store :refer [init-trunks dispose-trunks start-defrag stop-defrag
                                     init-durability-client]]
            [neb.base :refer [schemas-lock]]))

(def cluster-config-fields [:trunks-size])
(def cluster-confiugres (atom nil))
(def confiugres (atom nil))

(defn shutdown []
  (let [{:keys [schema-file]} @confiugres]
    (when schema-file (s/save-schemas schema-file))))

(defn stop-server []
  (println "Shutdowning...")
  (try-all
    (rfi/stop-server)
    (stop-defrag)
    (dispose-trunks)
    (leave-cluster)
    (shutdown)))

(defn interpret-volume [str-volume]
  (if (number? str-volume)
    str-volume
    (let [str-volume (clojure.string/lower-case (str str-volume))
          num-part (re-find #"\d+" str-volume)
          unit-part (first (re-find #"[a-zA-Z]+" str-volume))
          multiply (Math/pow
                     1024
                     (case unit-part
                       \k 1 \m 2 \g 3 \t 4 0))]
      (long (* (read-string num-part) multiply)))))

(defn get-cluster-configures []
  @cluster-confiugres)

(defn start-server [config]
  (let [{:keys [server-name port zk meta]} config]
    (join-cluster
      :neb
      server-name
      port zk meta
      :connected-fn
      (fn []
        (let [is-first-node? (is-first-node?)
              cluster-configs (select-keys config cluster-config-fields)
              cluster-configs (if is-first-node?
                                cluster-configs
                                (or (rfi/condinated-siblings-invoke 'neb.core/get-cluster-configures)
                                    cluster-configs))
              {:keys [trunks-size]} cluster-configs
              {:keys [memory-size schema-file data-path durability auto-backsync replication]} config
              trunks-size (interpret-volume trunks-size)
              memory-size (interpret-volume memory-size)
              schemas (if is-first-node?
                        (s/load-schemas-file schema-file)
                        (rfi/condinated-siblings-invoke-with-lock schemas-lock 'neb.schema/get-schemas))
              trunk-count (int (Math/floor (/ memory-size trunks-size)))]
          (println "Loading Store...")
          (reset! cluster-confiugres cluster-configs)
          (reset! confiugres config)
          (s/clear-schemas)
          (s/load-schemas schemas)
          (init-trunks trunk-count trunks-size (boolean durability))
          (when data-path (dserv/prepare-backup-server data-path))
          (when durability (init-durability-client (or replication 1)))
          (start-defrag)
          (register-as-master (* 50 trunk-count))
          (rfi/start-server port)))
      :expired-fn
      (fn []
        (stop-server)))))

(defn clear-zk []
  (delete-configure :schemas)
  (delete-configure :neb))