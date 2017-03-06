(ns thamesstream.windows
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :as log]
            [thamesstream
             [clients :as clients]
             [edn-serde :refer [edn-serde]]
             [kstreams :as k]
             [utils :refer [to-long-ts]]])
  (:import org.apache.kafka.common.serialization.Serdes
           org.apache.kafka.streams.state.QueryableStoreTypes
           org.apache.kafka.streams.StreamsConfig))

;;
;; transaction needs to have approval from at least two supervisors within time period
;;

(def ^:private store-name "transaction-store")

(defn- decision-made-by
  [approved? transactions]
  (-> (group-by :approve transactions)
      (get approved?)
      (#(map :decided-by %))))

(defn- build-approval
  [transaction-rec]
  {:key   (:key transaction-rec)
   :value {:approved   true
           :approved-by (->> transaction-rec
                            :value
                            (filter :approve)
                            (map :decided-by))}})

(defn- build-rejection
  [transaction-rec]
  (let [transaction (:value transaction-rec)]
    {:key   (:key transaction-rec)
     :value {:approved    false
             :rejected-by (decision-made-by false transaction)
             :approved-by (decision-made-by true transaction)}}))

(defn- approve?
  [transactions]
  (let [grouped-recs (group-by :approve transactions)
        approved (count (get grouped-recs true))
        declined (count (get grouped-recs false))]
    (and (>= approved 2) (zero? declined))))

(defn- forward
  [context result-rec]
  (log/info "Forwarding decision result " result-rec)
  (.forward context (:key result-rec) (:value result-rec)))

(defn- transform-fn
  [context k v]
  (let [store (.getStateStore @context "transaction-store")
        transactions (or (.get store k) [])]
    (log/info "Found transaction " transactions)
    (.put store k (-> transactions
                      (conj v)
                      distinct))))

(defn- punctuate-fn
  [context]
  (let [store (.getStateStore @context "transaction-store")
        transaction-recs (k/all store)]
    (log/info transaction-recs)
    (doseq [transaction-rec transaction-recs]
      (.delete store (:key transaction-rec))
      ;; blehh ugly
      (if (approve? (:value transaction-rec))
        (forward @context (build-approval transaction-rec))
        (forward @context (build-rejection transaction-rec))))))

(def ^:private transaction-transformer
  (k/supply-transformer (fn [_])
                        #'transform-fn
                        #'punctuate-fn
                        (fn [_])
                        {:schedule-time 10000}))

(defn- topology
  [builder]
  (.. builder
      (addStateStore (k/build-store store-name) (into-array String []))
      (stream (Serdes/String) (edn-serde) (into-array String ["transaction"]))
      (transform transaction-transformer (into-array String [store-name]))
      (to (Serdes/String) (edn-serde) "transaction-decided"))
  builder)

(defn streams
  []
  (->
   (k/kstream-builder)
   topology
   (k/kafka-streams (k/streams-config
                     "my-app7"
                     {StreamsConfig/TIMESTAMP_EXTRACTOR_CLASS_CONFIG
                      "thamesstream.timestamp_extractor.TimestamptExtractor"}))))

(comment

  (def builder
    (k/kstream-builder))

  (def st (streams))

  (.start st)

  (.close st)

  ;; inspect store
  (def view
    (.store st "transaction-store" (QueryableStoreTypes/keyValueStore)))

  (k/all view)
  (.get view "00000001")

  (do
    (clients/send "transaction" "00000001" {:decided-by "Peter" :approve true :published-at (to-long-ts 10 00 00)})
    (clients/send "transaction" "00000001" {:decided-by "Frank" :approve true :published-at (to-long-ts 11 00 01)})
    (clients/send "transaction" "00000001" {:decided-by "Stephan" :approve true :published-at (to-long-ts 11 00 10)})

    ;; trigger punctuate
    (clients/send "transaction" "00000009" {:decided-by "Drake" :approve false :published-at (to-long-ts 13 00 10)})
    )
  )
