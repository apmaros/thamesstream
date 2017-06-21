(ns thamesstream.clients
  (:require [cheshire.core :refer [generate-string]]
            [thamesstream
             [config :as c]
             [edn-serde :as edn]])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerRecord]
           org.apache.kafka.common.serialization.StringSerializer))

(def producer-config
  {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG c/broker-address})

(defn producer
  ([]
   (producer (StringSerializer.) (edn/edn-serializer)))
  ([key-serilizer value-serilizer]
   (KafkaProducer. producer-config
                   key-serilizer
                   value-serilizer)))

(defn send
  ([producer topic k v]
   (.send producer (ProducerRecord. topic (.toString k) v)))
  ([topic k v]
   (.send (producer) (ProducerRecord. topic (.toString k) v))))
