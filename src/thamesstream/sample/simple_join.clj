(ns thamesstream.sample.simple-join
  (:require [clojure.tools.logging :as log]
            [thamesstream
             [admin :as admin]
             [config :as config]
             [clients :as clients]
             [kstreams :as ks]]
            [thamesstream.edn-serde :as edn]))

(def bank-transaction-topic "bank-transaction")
(def bank-transaction-store "bank-transaction-store")
(def transaction-reconciled-topic "transaction-reconciled")
(def repayment-topic "repayment")

(defn reconcile?
  [v1 v2]
  (= (:amount v1) (:amount v2)))

(defn topology
  [builder]
  (->
   builder
   (ks/stream repayment-topic)
   (ks/left-join (ks/table builder
                           bank-transaction-topic
                           bank-transaction-store)
                 (fn [v1 v2]
                   (log/info v1 " - " v2)
                   {:bank-transaction v1
                    :repayment v2
                    :reconciles (reconcile? v1 v2)}))
   (ks/to transaction-reconciled-topic))
  builder)

(defn streams
  [application-id]
  (->
   (ks/kstream-builder)
   topology
   (ks/kafka-streams (ks/streams-config application-id))))

(comment


  ;; create topics if they don't exist
  (with-open [zk-utils (admin/zk-utils config/zk-connect-string)]
    (doall (map (fn [topic]
                  (admin/create-topic zk-utils topic 50 3))
                [bank-transaction-topic
                 repayment-topic
                 transaction-reconciled-topic])))

  ;; before running topology, make sure you have kafka cluster running
  (do
    (def s (streams "transaction-join"))

    (.start s)

    ;; publish messages
    (do
      (def p (clients/producer (edn/edn-serializer) (edn/edn-serializer)))
      (clients/send p bank-transaction-topic {:sort-code "2020" :id "1"} {:amount 1000})
      (clients/send p repayment-topic {:sort-code "2020" :id "1"} {:amount 1000}))

    (Thread/sleep (* 30 1000))

    (.close s))

  )
