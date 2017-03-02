(ns thamesstream.kstreams
  (:require [thamesstream.edn-serde :refer [edn-serde]])
  (:import org.apache.kafka.clients.consumer.ConsumerConfig
           org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams KafkaStreams StreamsConfig]
           org.apache.kafka.streams.kstream.KStreamBuilder
           org.apache.kafka.streams.processor.StateStoreSupplier
           org.apache.kafka.streams.state.Stores))

(defn streams-config
  ([application-id]
   (streams-config application-id {}))
  ([application-id conf]
   (->
    {StreamsConfig/APPLICATION_ID_CONFIG,    application-id
     ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"
     StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, (or (System/getenv "KAFKA_BOOTSTRAP_SERVERS")
                                                 "localhost:9092")}
    (merge conf)
    StreamsConfig.)))

(def kstream-builder
  (fn [] (KStreamBuilder.)))

(defn kafka-streams
  [builder config]
  (KafkaStreams. builder config))

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
   (build-store name {}))
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
