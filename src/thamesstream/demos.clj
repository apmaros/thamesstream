(ns thamesstream.demos
  (:require [taoensso.timbre :as log])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams KafkaStreams StreamsConfig]
           [org.apache.kafka.streams.kstream Aggregator Initializer JoinWindows KStreamBuilder ValueJoiner]
           org.apache.kafka.streams.kstream.Initializer))

(def properties
  {StreamsConfig/APPLICATION_ID_CONFIG,    "demo-world"
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, (or (System/getenv "KAFKA_BOOTSTRAP_SERVERS")
                                               "localhost:9092")
   StreamsConfig/KEY_SERDE_CLASS_CONFIG,   (.getName (.getClass (Serdes/String)))
   StreamsConfig/VALUE_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))})

(def config
  (StreamsConfig. properties))

(def builder
  (KStreamBuilder.))

(def input-topic
  (into-array String ["in"]))

;; stream
(def in-stream
  (.stream builder input-topic))

(def stream-processor
  (-> in-stream
      (.aggregateByKey (reify Initializer (apply [this]
                                            []))
                       (reify Aggregator (apply [this k v aggregate]
                                           (log/debug (format "Aggregated %s/%s %s"
                                                              key v aggregate))
                                           (into aggregate v)))
                       "inst-aggregated")))

(-> stream-processor
    (.print))

(def stream
  (KafkaStreams. builder config))

(comment
  (.start stream)

  (.close stream)


  )
