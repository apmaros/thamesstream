(ns thamesstream.core
  (:gen-class)
  (:require [clojure.string :refer [split]])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams KafkaStreams KeyValue StreamsConfig]
           [org.apache.kafka.streams.kstream KeyValueMapper KStreamBuilder ValueMapper]))

(def properties
  {StreamsConfig/APPLICATION_ID_CONFIG,    "word-counts"
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, (or (System/getenv "KAFKA_BOOTSTRAP_SERVERS")
                                               "localhost:9092")
   StreamsConfig/KEY_SERDE_CLASS_CONFIG,   (.getName (.getClass (Serdes/String)))
   StreamsConfig/VALUE_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))})

(def config
  (StreamsConfig. properties))

(def builder
  (KStreamBuilder.))

(def input-topic
  (into-array String ["in-t"]))

;; stream
(def text-lines
  (.stream builder input-topic))

;; stream processing
(defn count-words []
  (->
   text-lines
   (.flatMapValues
    (reify ValueMapper (apply [_ v]
                         (split v #" "))))
   (.map
    (reify KeyValueMapper (apply [_ v x]
                            (KeyValue. x x))))
   (.countByKey (Serdes/String) "Counts")
   (.toStream) ;; Transform KTable back Stream, not sure if needed
   (.to (Serdes/String) (Serdes/Long) "out-t")))

(count-words)

(defn stream
  []
  (KafkaStreams. builder config))

(defn start-stream
  []
  (.start (stream)))

(defn -main
  []
  (start-stream)
  (println "Hello, World!"))

(comment
  (.close (stream))

  )
