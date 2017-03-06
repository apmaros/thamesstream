(ns thamesstream.timestamp-extractor
  (:import org.apache.kafka.streams.processor.TimestampExtractor))

(deftype TimestamptExtractor []
  TimestampExtractor
  (extract [_ message foo]
    (let [message-timestamp  (:published-at (.value message))]
      (when message-timestamp
        (prn "Applying timestamp from message: " message-timestamp))
      (or message-timestamp (.timestamp message)))))
