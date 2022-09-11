(ns hooray.fixtures
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.edn :as edn]
            [hooray.db :as db]))

(def ^:dynamic *conn* nil)

(def ^:private test-data (edn/read-string (slurp "resources/transactions.edn")))

(defn unique-db-name [] (gensym "hooray-test-db"))

(defn mem-db-str [] (str "hooray:mem://" (unique-db-name)))

(defn with-chinook-db [f]
  (binding [*conn* (db/connect (mem-db-str))]
    (db/transact *conn* test-data)
    (f)))
