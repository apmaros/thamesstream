(ns thamesstream.windows
  (:require [clojure.tools.logging :as log]
            [thamesstream
             [clients :as clients]
             [edn-serde :refer [edn-serde]]
             [kstreams :as k]
             [utils :refer [to-long-ts]]])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams.kstream Aggregator Initializer KeyValueMapper Merger SessionWindows]
           org.apache.kafka.streams.state.QueryableStoreTypes))

;; http://docs.confluent.io/3.1.1/streams/concepts.html#windowing
;; Windowing is essentially dividing records into buckets.
;; It is typically used for stateful such join and aggregate.
;; The state is maintained in local store.

;; In Kafka Streams DSL, user defines the retention period for messages,
;; which arrive late (late arrival). Messages after this time are dropped

;; # Session Window with aggregate
;; Sessions represent a period of activity separated by a defined gap of inactivity

;; Case
;; Customer put order.
;; If no order is made within 5 seconds orders are processed and served.

;; Any events processed that fall within the inactivity gap of any existing sessions
;; are merged into the existing sessions

(def inactivity-gap
  5000)

(def retention-period
  10000)

(defn session-topology
  [builder]
  (.. builder
      (stream (Serdes/String) (edn-serde) (into-array String ["order"]))
      (groupByKey (Serdes/String) (edn-serde))
      (aggregate (reify Initializer
                          ;; `Initializer` is applied once before the first input is processed
                          (apply [_]
                            []))
                 (reify Aggregator
                   ;; `Aggregator` applied for each record to compute new aggregate
                   (apply [_ k v v-agg]
                     (-> v-agg
                         (conj v)
                         distinct)))
                 (reify Merger
                   ;; `Merger` is merging aggregate values for SessionWindows with the given key
                   (apply [_ k agg1 agg2]
                     (log/info "MERGING " agg1 " with: " agg2)
                     (prn "agg1::" agg1 " agg2::" agg2)
                     (conj agg2 agg1)))
                 ;; window for 5 seconds with retention period 01 seconds
                 (-> (SessionWindows/with inactivity-gap)
                     (.until retention-period))
                 (edn-serde)
                 "session-agg-orders")
      toStream
      (map (reify KeyValueMapper
             (apply [_ k v]
               (let [window (.window k)]
                 (log/info (.key k) "@" (.start window) "->" (.end window) "::" v)
                 (k/key-value k v))))))
  builder)

(defn streams
  []
  (->
   (k/kstream-builder)
   session-topology
   (k/kafka-streams (k/streams-config
                     "session-window-app2"))))

(comment

  (def builder
    (k/kstream-builder))

  (def st (streams))

  (.start st)

  (.close st)

  (def view
    (.store st "session-agg-orders" (QueryableStoreTypes/sessionStore)))

  (def e (first (map k/rec->map (iterator-seq (.fetch view "0000033")))))

  (map :value (map k/rec->map (iterator-seq (.fetch view "000033"))))

  (.key (:key e))

  (def ew (.window (:key e)))

  (.end ew)

  (.start ew)

  (.close st)

  ;; SESSION I
  (clients/send "order" "000033" {:decided-by "Peter" :approve true :published-at (to-long-ts 10 10 00)})
  ;;Merger
  ;; "agg1::" [] " agg2::" ({:decided-by "Peter", :approve true, :published-at 1489572600000})

  ;; 000033 @ 1489572600000 -> 1489572604000 :: ({:decided-by James, :approve true, :published-at 1489572604000} [] {:decided-by Peter, :approve true, :published-at 1489572600000})

  (clients/send "order" "000033" {:decided-by "James" :approve true :published-at (to-long-ts 10 10 04)})
  (clients/send "order" "000033" {:decided-by "Daniel" :approve true :published-at (to-long-ts 10 10 9)})
  ;; Merger
  ;; "agg1::" [] " agg2::" ({:decided-by "James", :approve true, :published-at 1489572604000} [] {:decided-by "Peter", :approve true, :published-at 1489572600000})

  ;; 000033 @ 1489572600000 -> 1489572609000 :: ({:decided-by Daniel, :approve true, :published-at 1489572609000} [] {:decided-by James, :approve true, :published-at 1489572604000} {:decided-by Peter, :approve true, :published-at 1489572600000})

  ;; (nil
  ;;  nil
  ;;  ({:decided-by "Daniel", :approve true, :published-at 1489572609000}
  ;;   []
  ;;   {:decided-by "James", :approve true, :published-at 1489572604000}
  ;;   {:decided-by "Peter", :approve true, :published-at 1489572600000}))
  )
