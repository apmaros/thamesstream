(ns thamesstream.kstreams
  (:require [clojure.tools.logging :as log]
            [thamesstream.edn-serde :refer [edn-serde]])
  (:import org.apache.kafka.clients.consumer.ConsumerConfig
           org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams KafkaStreams KeyValue StreamsConfig]
           [org.apache.kafka.streams.kstream KStreamBuilder Transformer TransformerSupplier]
           org.apache.kafka.streams.processor.StateStoreSupplier
           org.apache.kafka.streams.state.Stores))

;; KS
(def kstream-builder
  (fn [] (KStreamBuilder.)))

(defn kafka-streams
  [builder config]
  (KafkaStreams. builder config))

(defn streams-config
  ([application-id]
   (streams-config application-id {}))
  ([application-id conf]
   (->
    {StreamsConfig/APPLICATION_ID_CONFIG,           application-id
     StreamsConfig/TIMESTAMP_EXTRACTOR_CLASS_CONFIG "thamesstream.timestamp_extractor.TimestamptExtractor"
     ConsumerConfig/AUTO_OFFSET_RESET_CONFIG        "earliest"
     StreamsConfig/BOOTSTRAP_SERVERS_CONFIG,        (or (System/getenv "KAFKA_BOOTSTRAP_SERVERS")
                                                        "localhost:9092")
     StreamsConfig/CACHE_MAX_BYTES_BUFFERING_CONFIG  0}
    (merge conf)
    StreamsConfig.)))

;; Store
(defn rec->map
  [rec]
  {:key   (.key rec)
   :value (.value rec)})

(defn all
  [store]
  (->>
   (.all store)
   iterator-seq
   (map rec->map)))

(defn build-store
  ([name]
   (build-store name {:key-serde (Serdes/String) :value-serde (edn-serde)}))
  ([name {:keys [key-serde value-serde] :as serdes}]
   (..
    (Stores/create name)
    (withKeys key-serde)
    (withValues value-serde)
    (persistent)
    (build))))

(defn supply-store
  [store-name]
  (reify
    StateStoreSupplier
    (get [_]
      (..
       (Stores/create store-name)
       (withKeys (Serdes/String))
       (withValues (edn-serde))
       persistent
       build)
      (name [_]
            store-name))))

(defn key-value
  [k v]
  (KeyValue/pair k v))

;; Topology
(defn supply-transformer
  [init-fn transform-fn punct-fn close-fn config]
  (reify TransformerSupplier
    (get [_]
      (let [context (atom nil)]
        (reify Transformer
          (init [_ transformer-context]
            (some->>
             (:schedule-time config)
             (.schedule transformer-context))
            (reset! context transformer-context)
            (init-fn transformer-context))

          (transform [_ k v]
            (log/info (format "[transformer] processing %s, %s" k v))
            (transform-fn context k v))

          (punctuate [_ timestamp]
            (log/info "[transformer] Running scheduled task at time " timestamp)
            (punct-fn context))

          (close [_]
            (log/info "[transformer] Closing transformer...")
            (close-fn context)))))))
