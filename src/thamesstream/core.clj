(ns thamesstream.core
  (:require [clojure.string :refer [split]])
  (:import
           org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams KafkaStreams KeyValue StreamsConfig]
           [org.apache.kafka.streams.kstream KeyValueMapper KStreamBuilder ValueMapper]))

(def properties
  {StreamsConfig/APPLICATION_ID_CONFIG,    "my-stream-processing-application"
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, (or (System/getenv "KAFKA_BOOTSTRAP_SERVERS") "localhost:9092")
   StreamsConfig/KEY_SERDE_CLASS_CONFIG,   (.getName (.getClass (Serdes/String)))
   StreamsConfig/VALUE_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))})

(def config
  (StreamsConfig. properties))

(def builder
  (KStreamBuilder.))

(def input-topic
  (into-array String ["in-t"]))

(def lines
  (.stream builder input-topic))

(def string-serde
  (Serdes/String))

(defn word-counts
  [stream]
  (->
   stream
   (.flatMapValues
    (reify ValueMapper (apply [_ v]
                         (split v #" "))))
   (.map
    (reify KeyValueMapper (apply [_ v x]
                            (KeyValue. x x))))
   (.countByKey (Serdes/String) "Counts")
   (.toStream) ;; Transform KTable back Stream
   (.to (Serdes/String) (Serdes/Long) "out-t")))

(word-counts lines)

(.print lines)

(def streams
  (KafkaStreams. builder config))

(defn close-stream
  []
  (.close streams))

(defn start-stream
  []
  (.start streams))

(comment
  (split " " #" "))
