(ns thamesstream.branch
  (:require [thamesstream
             [clients :as client]
             [edn-serde :refer [edn-serde]]
             [kstreams :as ks]])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams.kstream KeyValueMapper Predicate]))

;; Use case
;; divide stream of transactions into specialised topics for further processing if state is confirmed
;; input: transaction - <id: {type: retail | bank | cash, :status confirmed | pending | cancelled }>
;; output: retail-transaction, bank-transaction, cash-transaction

(def ^:private confirmed-status "confirmed")

(def bank-topic "bank-transactions")
(def retail-topic "retail-transactions")
(def cash-topic "cash-transactions")
(def transactions-topic "transactions")

(def bank-type?
  (ks/predicate (fn [k v]
                  (= (:type v) "bank"))))

(def retail-type?
  (ks/predicate (fn [k v]
                  (= (:type v) "retail"))))

(def cash-type?
  (ks/predicate (fn [k v]
                  (= (:type v) "cash"))))

(defn topology
  [builder]
  (let [[bank-stream
         retail-stream
         cash-stream] (.. builder
                          (stream (Serdes/String) (edn-serde) (into-array String [transactions-topic]))

                          (map ks/log-stream)

                          (filter (ks/predicate (fn [k v]
                                                  (= (:status v) confirmed-status))))

                          (branch (ks/predicates [bank-type?
                                                  retail-type?
                                                  cash-type?])))]

    (doall (map (fn [stream topic]
                  (.to stream (Serdes/String) (edn-serde) topic))
                [bank-stream retail-stream cash-stream]
                [bank-topic retail-topic cash-topic])))
  builder)


(defn streams
  [application-id]
  (->
   (ks/kstream-builder)
   topology
   (ks/kafka-streams (ks/streams-config application-id))))

(comment

  (def s (streams "foo"))

  (.start s)

  (.close s)

  (client/send "transactions" "001" {:type "cash", :status "confirmed" :account-id "00001234"})

  (client/send "transactions" "001" {:type "retail", :status "confirmed" :account-id "0000567"})

  (client/send "transactions" "001" {:type "retail", :status "pending" :account-id "0000567"})
  )
