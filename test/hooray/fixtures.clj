(ns hooray.fixtures
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.edn :as edn]
            [hooray.db :as db]
            [hooray.graph :as g]
            [hooray.db.memory.graph :as mem-gr]
            [hooray.db.memory.graph-index :as mem-gi]))

(def ^:dynamic *conn* nil)
(def ^:dynamic *graph* nil)

(def ^:private test-data (edn/read-string (slurp "resources/transactions.edn")))

(defn unique-db-name [] (gensym "hooray-test-db"))

(defn mem-db-str [] (str "hooray:mem://" (unique-db-name)))

(defn with-chinook-db [f]
  (binding [*conn* (db/connect (mem-db-str))]
    (db/transact *conn* test-data)
    (f)))

(def ^:dynamic *graph-type* nil)

(defn with-chinook-index-graph [f]
  (binding [*graph* (mem-gi/memory-graph {:type (or *graph-type* :simple)})]
    (g/transact *graph* test-data)
    (f)))
