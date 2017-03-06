(ns thamesstream.clients
  (:require [thamesstream
             [config :as c]
             [edn-serde :as edns]])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerRecord]
           org.apache.kafka.common.serialization.StringSerializer
           org.apache.kafka.streams.KeyValue))

(def producer-config
  {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG c/broker-address})

(defn producer
  []
  (KafkaProducer. producer-config
                  (StringSerializer.)
                  (edns/edn-serializer)))

(defn send
  [topic k v]
  (.send (producer) (ProducerRecord. topic (.toString k) v)))
