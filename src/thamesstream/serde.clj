(ns thamesstream.serde
  (:gen-class
   :name custom.Serde
   :main false
   :implements [org.apache.kafka.common.serialization.Serde]
   :init build-serde
   :state state
   :constructors {[] [], [io.confluent.kafka.schemaregistry.client.SchemaRegistryClient] [], [io.confluent.kafka.schemaregistry.client.SchemaRegistryClient java.util.Map] []})
  (:import [io.confluent.examples.streams.utils GenericAvroDeserializer GenericAvroSerializer]
           io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
           [org.apache.kafka.common.serialization Deserializer Serdes]))

(defn deserializer-record [r]
  (let [schema-name (.. r
                        (getSchema)
                        (getProp "public_name")
                        (replaceAll "_" "-"))
         schema-class   "missing" ;(avs/load-janus-schema schema-name)
         schema-instance "missing too"] ; (scc/from-avro schema-class r)]
    ;; (avs/validate schema-class schema-instance)
    schema-instance))

(defn deserializer-for [d]
  (reify Deserializer
    (configure [this m k]
      (.configure d m k))
    (close [this]
      (.close d))
    (deserialize [this s byte]
      (deserializer-record (.deserialize d s byte))
      )))


(defn trace [x]
  (prn "RECEIVED" x)
  x)

(defn -build-serde
  ([]
   (let [deserializer (deserializer-for (GenericAvroDeserializer.))]
     [[] {:inner (Serdes/serdeFrom (GenericAvroSerializer.) deserializer)}]))
  ([^SchemaRegistryClient client]
   (let [deserializer (deserializer-for (GenericAvroDeserializer. client {}))]
     [[] {:inner (Serdes/serdeFrom (GenericAvroSerializer. client) deserializer)}]))
  ([^SchemaRegistryClient client ^java.util.Map m]
   (let [deserializer (deserializer-for (GenericAvroDeserializer. client m))]
     [[] {:inner (Serdes/serdeFrom (GenericAvroSerializer. client) deserializer)}])))

(defn -serializer
  [this]
  (.serializer (:inner (.state this))))

(defn -deserializer
  [this]
  (.deserializer (:inner (.state this))))

(defn -configure
  [this config k]
  (let [{:keys [inner]} (.state this)]
    (.configure (.serializer inner) config k)
    (.configure (.deserializer inner) config k)))

(defn -close
  [this]
  (let [{:keys [inner]} (.state this)]
    (.close (.serializer inner))
    (.close (.deserializer inner))))
