(ns thamesstream.edn-serde
  (:import [org.apache.kafka.common.serialization Deserializer Serde StringDeserializer StringSerializer]))

(defn edn-serializer []
  (proxy [StringSerializer] []
      (serialize [topic data]
            (proxy-super serialize topic (pr-str data)))))

(defn edn-deserializer []
  (let [string-deserializer (StringDeserializer.)]
    (reify Deserializer
      (deserialize [_ topic data]
        (when data
          (read-string (.deserialize string-deserializer topic data)))))))

(deftype EDNSerde []
  Serde
  (configure [_ configs is-key])
  (close [_])
  (serializer [_]
    (edn-serializer))
  (deserializer [_]
    (edn-deserializer)))

(defn edn-serde []
  (EDNSerde.))
