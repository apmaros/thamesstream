(ns thamesstream.windows
  (:require [clj-time
             [coerce :as tc]
             [core :as t]]
            [clojure.tools.logging :as log]
            [thamesstream
             [clients :as clients]
             [edn-serde :refer [edn-serde]]
             [kstreams :as k]
             [utils :refer [to-long-ts]]])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams KeyValue StreamsConfig]
           [org.apache.kafka.streams.kstream Transformer TransformerSupplier]))

(comment

  (def builder
    (k/kstream-builder))

  (defn my-transformer
    []
    (reify TransformerSupplier
      (get [_]
        (let [context (atom nil)]
          (reify Transformer
            (init [_ transformer-context]
              (.schedule transformer-context 5000)
              (reset! context transformer-context))
            (transform [_ k v]
             (log/info ">>>>>>>>>>>>>>>>>>>>>>")
              (log/info "Transforming..." k "-" v)
              (KeyValue/pair k v))
            (punctuate [_ timestamp]
              (log/info ">>>>>>>>>>>>>>>>>>>>>>")
              (log/info "TIME:: " timestamp)
              (log/info ">>>>>>>>>>>>>>>>>>>>>>"))
            (close [_]))))))

  (def builder (k/kstream-builder))

  (def topology
    (.. builder
        (addStateStore (k/build-store "my-store") (into-array String []))
        (stream (Serdes/String) (edn-serde) (into-array String ["topic6"]))
        (transform (my-transformer) (into-array String ["my-store"]))))

  (def stream
    (k/kafka-streams builder (k/streams-config
                              "my-app6"
                              {StreamsConfig/TIMESTAMP_EXTRACTOR_CLASS_CONFIG
                               "thamesstream.timestamp_extractor.TimestamptExtractor"})))


  (.start stream)

  (.close stream)

  (clients/send "topic6" 1 {:foo "bar" :published-at (to-long-ts 10 10 10)})
  ;; triggered window

  (clients/send "topic6" 1 {:foo "bar" :published-at (to-long-ts 10 10 10)})
  ;; didn't trigger window

  (clients/send "topic6" 1 {:foo "bar" :published-at (to-long-ts 10 10 15)})
  ;; triggered window with timestamp 1488449415000

  (clients/send "topic6" 3 {:foo "bar" :published (to-long-ts 10 10 17)})

  (clients/send "topic1" 3 {:foo "bar" :published (to-long-ts 10 10 30)})


  )
