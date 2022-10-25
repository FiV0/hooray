(ns hooray.util
  (:require [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [hooray.constants :as constants])
  (:import (java.lang IllegalArgumentException UnsupportedOperationException)))

(defn read-edn [f]
  (-> (slurp f)
      edn/read-string))

(comment
  (read-edn "resources/transactions.edn")
  )

(defn now [] (java.util.Date.))

;; copied from clojure.incubator
(defn dissoc-in
  "Dissociates an entry from a nested associative structure returning a new
  nested structure. keys is a sequence of keys. Any empty maps that result
  will not be present in the new structure."
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(comment
  (def a {:a {:b #{:c}}})
  (def b {:a {:b {:c :d} :foo {}} :d {}})
  (dissoc-in b [:a :b :c])

  )

(defn variable? [v]
  (and (symbol? v) (= \? (first (name v)))))

(defn wildcard? [v]
  (= v '_))

(defn constant? [v]
  (not (or (wildcard? v) (variable? v))))

(defn unsupported-ex
  ([] (UnsupportedOperationException.))
  ([msg] (UnsupportedOperationException. msg)))

(defn illegal-ex
  ([] (IllegalArgumentException.))
  ([msg] (IllegalArgumentException. msg)))

(defn format-time
  "Formats the given timestamp in human readable format."
  ([time-ms] (format-time "" time-ms))
  ([format-str time-ms]
   (cond
     (>= time-ms constants/year)
     (format-time (str format-str " " (quot time-ms constants/year) "years")
                  (mod time-ms constants/year))

     (>= time-ms constants/day)
     (format-time (str format-str " " (quot time-ms constants/day) "days")
                  (mod time-ms constants/day))

     (>= time-ms constants/hour)
     (format-time (str format-str " " (quot time-ms constants/hour) "hours")
                  (mod time-ms constants/hour))

     (>= time-ms constants/minute)
     (format-time (str format-str " " (quot time-ms constants/minute) "minutes")
                  (mod time-ms constants/minute))

     (>= time-ms constants/sec)
     (format-time (str format-str " " (quot time-ms constants/sec) "seconds")
                  (mod time-ms constants/sec))

     :else
     (str format-str " " time-ms  "milliseconds"))))

(defn print-time
  "Prints the given timestamp in human readable format."
  [time-ms]
  (println (format-time time-ms)))

(comment
  (print-time (System/currentTimeMillis)))

(defn with-timing* [f]
  (let [start-time-ms (System/currentTimeMillis)
        ret (try
              (f)
              (catch Exception e
                (log/error e "caught exception during")
                {:error (str e)}))]
    {:res ret
     :ms (- (System/currentTimeMillis) start-time-ms)}))

(defmacro with-timing [& body]
  `(with-timing* (fn [] ~@body)))
