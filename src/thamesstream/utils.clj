(ns thamesstream.utils
  (:require [clj-time
             [coerce :as tc]
             [core :as t]]
            [clojure.walk :refer [stringify-keys]])
  (:import java.util.Properties))

(def stream-store (atom nil))

(def to-long-ts
    (comp tc/to-long t/today-at))

(defn set-stream
  [stream]
  (swap! stream (constantly stream)))

(defn ->properties
  [m]
  (doto (java.util.Properties.)
    (.putAll (stringify-keys m))))
