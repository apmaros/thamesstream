(ns thamesstream.utils
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]))

(def stream-store (atom nil))

(def to-long-ts
    (comp tc/to-long t/today-at))

(defn set-stream
  [stream]
  (swap! stream (constantly stream)))
