(ns hooray.util
  (:require [clojure.edn :as edn]))

(defn read-edn [f]
  (-> (slurp f)
      edn/read-string))

(comment
  (read-edn "resources/transactions.edn")
  )

(defn now [] (java.util.Date.))
