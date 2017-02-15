(ns thamesstream.processor-api
  "Based on [Confluent example](http://docs.confluent.io/3.1.0/streams/developer-guide.html#processor-api)"
  (:require [taoensso.timbre :as log]
            [thamesstream.config :as config])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams KafkaStreams KeyValue StreamsConfig]
           [org.apache.kafka.streams.kstream KStreamBuilder Transformer TransformerSupplier]
           [org.apache.kafka.streams.processor Processor ProcessorSupplier StateStoreSupplier]
           org.apache.kafka.streams.state.internals.RocksDBStore
           org.apache.kafka.streams.state.Stores))

;; stream processor is a node in the processor topology that represents a single processing step
;; - receives one record at time.
;; - connects processors with their associated state stores - together compose processor topology

;; stream processor implements `Processor` interface

;; Word count example

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

(defn rec->map
  [rec]
  {:key (.key rec) :value (.value rec)})

(def context
  (atom nil))

(defn get-store
  [name]
  (.getStateStore @context name))

(def max-orders 5)

(def confirm-msg
  "Your order %s was processed and is in the queue %d")

(defn build-processor
  []
  (reify Processor
    ;; called during construction phase
    ;; useful to setup the context and schedule punctuate
    (init [_ init-context]
      (reset! context init-context)
      (.schedule @context 1000))

    ;; called on each of the received record.
    (process [this k v]
      (log/info "RECEIVED::" k "-" v)
      (let [food-store (get-store "food-store")
            order-counter (get-store "order-counter")
            order-count (.get order-counter v)]

        (log/info order-count " orders for " v)
        (cond
          (nil? order-count) (let [init-count 1]
                               (.put order-counter v init-count)
                               (.forward @context k (format confirm-msg v init-count)
                                         "orders-sink"))
          (>= order-count max-orders) (.forward @context k
                                                "Can not order, maximum orders reached!" "failed-orders-sink")
          (>= order-count 1) (let [updated-order-count (inc order-count)]
                               (log/info "Increasing order count to " updated-order-count)
                               (.put order-counter v updated-order-count)
                               (.forward @context k (format confirm-msg v updated-order-count) "orders-sink"))
          :else (throw (Exception. "Don't know what to do, no action matched")))

      (.putIfAbsent food-store k v)

      ;; commits current processing progress
      (.commit @context)
      ;; flush cached data to kafka topic
      (map #(.flush %) [food-store order-counter])))

    ;; called periodically based on elapsed
    (punctuate [this timestamp]
      (log/info "WINDOW::" timestamp))
    (close [_]
      (log/info "Closing topology...")
      (.close (get-store "words")))))

(defn words-topology
  [builder]
  (->
   builder
   ;; source processors generate input data streams into the topology
   (.addSource "source" (into-array String ["order-food"]))

   ;; processor node
   ;; connects with upstream node `source`
   (.addProcessor "process" (reify ProcessorSupplier
                             (get [_] (build-processor)))
                 (into-array String ["source"]))
   ;; store with `process` upstream processor node
   (.addStateStore (build-store "food-store") (into-array String ["process"]))
   (.addStateStore (build-store "order-counter" {:key-serde (Serdes/String) :value-serde (Serdes/Long)})
                  (into-array String ["process"]))

   ;; sink processors generate output data streams out of the topology
   ;; `process` is upstream processing node
   (.addSink "failed-orders-sink" "food-order-failed" (into-array String ["process"]))
   (.addSink "orders-sink" "food-ordered" (into-array String ["process"]))))


(defn supply-store
  [store-name]
  (reify
    StateStoreSupplier
    (get [_]
      (RocksDBStore. store-name (Serdes/String) (Serdes/String)))
    (name [_]
      store-name)))

;; Transformer instead of Processor
(defn supply-food-transformer
  []
  (reify TransformerSupplier
    (get [_]
      (reify Transformer
        (init [_ transformer-context])
        (transform [_ k v]
          (log/info "Transforming..." k "-" v)
          (KeyValue/pair k v))
        (punctuate [_ timestamp])
        (close [_])))))

(defn dsl-topology
  [builder]
  (let [_  (.addStateStore builder (supply-store "food-store") (into-array String []))]
    (.. builder
      (stream (into-array String ["order-food"]))
      (transform (supply-food-transformer)
                 (into-array String ["food-store"]))))
  builder)

(comment

  (defn builder
    []
    (KStreamBuilder.))

  (def topology
    (words-topology (builder)))

  (def dsl-topology
    (dsl-topology (builder)))

  (def streams (KafkaStreams. topology (StreamsConfig. (config/streams-properties "words-count-v1"))))

  (def streams (KafkaStreams. (dsl-topology (KStreamBuilder.)) (StreamsConfig. (config/streams-properties "dsl-words-count-v12"))))

  (.start streams)

  (.close streams)

  (.process (build-processor "foo") "foo" "bar")

  )
