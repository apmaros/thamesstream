(ns thamesstream.core
  (:require [clojure.string :refer [upper-case]])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams KafkaStreams StreamsConfig]
           [org.apache.kafka.streams.kstream KStreamBuilder ValueMapper]))

; import org.apache.kafka.common.serialization.Serde;
; import org.apache.kafka.common.serialization.Serdes;
; import org.apache.kafka.streams.KafkaStreams;
; import org.apache.kafka.streams.KeyValue;
; import org.apache.kafka.streams.StreamsConfig;
; import org.apache.kafka.streams.kstream.KStream;
; import org.apache.kafka.streams.kstream.KStreamBuilder;

(def props
  {StreamsConfig/APPLICATION_ID_CONFIG,    "my-stream-processing-application"
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
   StreamsConfig/KEY_SERDE_CLASS_CONFIG,   (.getName (.getClass (Serdes/String)))
   StreamsConfig/VALUE_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))})

(def config
  (StreamsConfig. props))

(def builder
  (KStreamBuilder.))

(def input-topic
  (into-array String ["in-t"]))

(->
 (.stream builder input-topic)
 (.mapValues (reify ValueMapper (apply [_ v] ((comp str count) v))))
 (.to "out-t"))

(def streams
  (KafkaStreams. builder config))

(defn close-stream
  []
  (.close streams))

(defn start-stream
  []
  (.start streams))

