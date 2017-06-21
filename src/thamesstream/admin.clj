(ns thamesstream.admin
  (:require [thamesstream.utils :refer [->properties]])
  (:import kafka.admin.AdminUtils
           kafka.utils.ZkUtils))

(defn zk-utils
  [connect-str & {:keys [zk-session-timeout-ms zk-connection-timeout-ms]
                  :or {zk-session-timeout-ms 10000
                       zk-connection-timeout-ms 8000}}]
  (ZkUtils/apply (ZkUtils/createZkClient connect-str
                                         zk-session-timeout-ms
                                         zk-connection-timeout-ms) false))

(defn create-topic
  ([zk-utils topic partitions replication]
   (create-topic zk-utils topic partitions replication {}))
  ([zk-utils topic partitions replication topic-config]
   (AdminUtils/createTopic zk-utils (name topic) partitions replication (->properties topic-config) nil)))
