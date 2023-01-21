(ns hooray.fixtures
  (:require [clojure.edn :as edn]
            [clojure.test :as test :refer [testing]]
            [clojure.tools.logging :as log]
            [hooray.core :as hooray]
            [hooray.db :as db]
            [hooray.db.memory.graph-index :as mem-gi]
            [hooray.db.persistent.redis :as redis]
            [hooray.graph :as g]
            [hooray.util :as util]
            [taoensso.carmine :as car]))

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
                        {:type :mem :sub-type :tonsky} "hooray:mem:tonsky//data"
                        ;; {:type :mem :sub-type :core :algo :generic} "hooray:mem:core:generic//data"
                        {:type :mem :sub-type :avl :algo :generic} "hooray:mem:avl:generic//data"
                        {:type :mem :sub-type :tonsky :algo :generic} "hooray:mem:tonsky:generic//data"

                        #_{:type :per :sub-type :redis :algo :leapfrog :spec {:uri "redis://localhost:6379/"}}
                        #_{:type :per :sub-type :redis :name "hello" :algo :leapfrog :spec {:uri "redis://localhost:6379/"}}

                        #_{:type :per :sub-type :redis :algo :generic :spec {:uri "redis://localhost:6379/"}}
                        #_{:type :per :sub-type :redis :name "hello" :algo :generic :spec {:uri "redis://localhost:6379/"}}})

(def ^:dynamic *db-context* nil)

(defn with-each-db-option* [f]
  (doseq [[ctx db-url] db-urls]
    (binding [*db-context* ctx
              *conn* (hooray/connect db-url)]
      (testing db-url
        (f))
      (when (satisfies? db/DropDB *conn*)
        (db/drop-db *conn*)))))

(defmacro with-each-db-option [& body]
  `(with-each-db-option* (fn [] ~@body)))

(defn with-timing [f]
  (let [start-time-ms (System/currentTimeMillis)
        ret (try
              (f)
              (catch Exception e
                (log/error e "caught exception during")
                {:error (str e)}))]
    (merge (when (map? ret) ret)
           {:time-taken-ms (- (System/currentTimeMillis) start-time-ms)})))

(defmacro with-timing* [& body]
  `(with-timing (fn [] ~@body)))

(defn with-timing-logged [f]
  (let [{:keys [time-taken-ms]} (with-timing f)]
    (log/infof "Test took %s" (util/format-time time-taken-ms))))

(defmacro with-timing-logged* [& body]
  `(let [{:keys [~'time-taken-ms]} (with-timing (fn [] ~@body))]
     (log/infof "Test took %s" (util/format-time ~'time-taken-ms))))

(defmacro deftest+timing
  "Just like `cloure.test/deftest` but timed and the first form is ignored.

  (deftest+timing complete-graph-test
    (h/transact *conn* (g/graph->ops (g/complete-graph 100)))
    (is (= 161700
           (count (h/q triangle-query (h/db *conn*))))))"
  [name setup & body]
  (when clojure.test/*load-tests*
    `(def ~(vary-meta name assoc :test
                      `(fn []
                         ~setup
                         (with-timing-logged* ~@body)))
       (fn [] (clojure.test/test-var (var ~name))))))

(defn with-println [f]
  (f)
  (println))

;;///////////////////////////////////////////////////////////////////////////////
;;===============================================================================
;;                                     Redis
;;===============================================================================
;;\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

(defonce ^:prviate my-conn-pool   (car/connection-pool {})) ; Create a new stateful pool
(def ^:private     my-conn-spec-1 {:uri "redis://localhost:6379/"})

(def redis-conn {:pool my-conn-pool :spec my-conn-spec-1})

(def ^:dynamic *keyspace* nil)
(def ^:dynamic *key-store* nil)

;; copied from clj_fdb
(let [alphabet (vec "abcdefghijklmnopqrstuvwxyz0123456789")]
  (defn rand-str
    "Generate a random string of length l"
    [l]
    (loop [n l res (transient [])]
      (if (zero? n)
        (apply str (persistent! res))
        (recur (dec n) (conj! res (alphabet (rand-int 36))))))))

(defn with-redis-keyspace [f]
  (binding [*keyspace* (str "test-keyspace" (rand-str 5))
            *key-store* (redis/->redis-key-store redis-conn)]
    (f)
    (redis/clear-set redis-conn *keyspace*)))
