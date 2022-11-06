(ns hooray.fixtures
  (:require [clojure.test :as test :refer [testing]]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [hooray.core :as hooray]
            [hooray.db :as db]
            [hooray.graph :as g]
            [hooray.db.memory.graph-index :as mem-gi]
            [hooray.util :as util]))

(def ^:dynamic *conn* nil)
(def ^:dynamic *graph* nil)

(def ^:private test-data (edn/read-string (slurp "resources/transactions.edn")))

(defn unique-db-name [] (gensym "hooray-test-db"))

(defn mem-db-str [] (str "hooray:mem://" (unique-db-name)))

(defn with-mem-db [f]
  (binding [*conn* (db/connect (mem-db-str))]
    (f)))

(defn with-chinook-data [f]
  (db/transact *conn* test-data)
  (f))

(def ^:dynamic *graph-type* nil)

(defn with-chinook-index-graph [f]
  (binding [*graph* (mem-gi/memory-graph {:type (or *graph-type* :simple)})]
    (g/transact *graph* test-data)
    (f)))

(defn with-identity-hash-index-graph [f]
  (let [old mem-gi/hash]
    (alter-var-root #'mem-gi/hash (constantly identity))
    (f)
    (alter-var-root #'mem-gi/hash (constantly old))))

(def ^:private db-urls {{:type :mem :sub-type nil} "hooray:mem://data"
                        {:type :mem :sub-type :core} "hooray:mem:core//data"
                        {:type :mem :sub-type :avl} "hooray:mem:avl//data"
                        {:type :mem :sub-type :tonsky} "hooray:mem:tonsky//data"})

(def ^:dynamic *db-context* nil)

(defn with-each-db-option* [f]
  (doseq [[ctx db-url] db-urls]
    (binding [*db-context* ctx
              *conn* (hooray/connect db-url)]
      (testing db-url
        (f)))))

(defmacro with-each-db-option [& body]
  `(with-each-db-option* (fn [] ~@body)))

(defn with-timing* [f]
  (let [start-time-ms (System/currentTimeMillis)
        ret (try
              (f)
              (catch Exception e
                (log/error e "caught exception during")
                {:error (str e)}))]
    (merge (when (map? ret) ret)
           {:time-taken-ms (- (System/currentTimeMillis) start-time-ms)})))

(defmacro with-timing [& body]
  `(with-timing* (fn [] ~@body)))

(defn with-timing-logged [f]
  (let [{:keys [time-taken-ms]} (with-timing* f)]
    (log/infof "Test took %s" (util/format-time time-taken-ms))))
