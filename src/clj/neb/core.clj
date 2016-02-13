(ns neb.core
  (:require [cluster-connector.remote-function-invocation.core :as rfi]
            [cluster-connector.distributed-store.core :refer [join-cluster with-cc-store leave-cluster] :as ds]
            [cluster-connector.sharding.core :refer [register-as-master checkout-as-master]]
            [cluster-connector.native-cache.core :refer :all]
            [cluster-connector.sharding.DHT :refer :all]
            [neb.schema :refer [load-schemas-file load-schemas]])
  (:import (java.util UUID)
           (com.google.common.hash Hashing MessageDigestHashFunction HashCode)
           (java.nio.charset Charset)))

(def cluster-config-fields [:trunks-size])

(defn start-server [svr-name port zk meta config]
  (join-cluster
    :neb
    (:name config)
    port zk meta
    :connected-fn
    (fn []
      (let [cluster-configs (select-keys config cluster-config-fields)
            cluster-configs (or (ds/get-configure :neb)
                                (do (ds/set-configure :neb cluster-configs)
                                    cluster-configs))
            {:keys [trunks-size]} cluster-configs
            {:keys [memory-size data-path]} config
            schemas (or (ds/get-configure :schemas)
                        (let [s (load-schemas-file data-path)]
                          (ds/set-configure :schemas s) s))
            trunk-count (int (Math/floor (/ memory-size trunks-size)))]

        (load-schemas schemas)
        (register-as-master (* 20 trunk-count))
        (rfi/start-server port)))
    :expired-fn
    (fn []
      (rfi/stop-server))))

(def stop-server leave-cluster)

(defn rand-cell-id [] (UUID/randomUUID))

(defn hash-str [string alog]
  (-> alog
      (.hashString string (Charset/forName "UTF-8"))
      (.asLong)))

(defn cell-id-by-key* [^String cell-key]
  (UUID.
    (hash-str cell-key (Hashing/sha1))
    (hash-str cell-key (Hashing/sha256))))

(defcache cell-id-by-key {:expire-after-access-secs :3600} cell-id-by-key*)

(defn locate-cell-by-id [^UUID cell-id]
  (get-server-for-name cell-id :hashing #(.getMostSignificantBits %)))

(defn cell-key-to-id [key]
  (if (= (class key) UUID)
    key
    (cell-id-by-key key)))

(defn- dist-call [cell-key func & params]
  (let [cell-id (cell-key-to-id cell-key)
        server-name (locate-cell-by-id cell-id)]
    (apply rfi/invoke server-name func cell-id params)))

(defn delete-cell [key]
  (dist-call key 'neb.trunk-store/delete-cell))

(defn read-cell [key]
  (dist-call key 'neb.trunk-store/read-cell))

(defn new-cell [key schema-id data]
  (dist-call key 'neb.trunk-store/new-cell schema-id data))

(defn replace-cell [key data]
  (dist-call key 'neb.trunk-store/replace-cell data))

(defn update-cell [key fn & params]
  (apply dist-call key 'neb.trunk-store/update-cell fn params))
