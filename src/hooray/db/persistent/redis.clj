(ns hooray.db.persistent.redis
  (:require [clojure.string :as str]
            [taoensso.carmine :as car :refer [wcar]]
            [taoensso.nippy :as nippy]))

;; API an persistent key/value store should support
;; set-kv store k
;; set-kvs store ks
;; delete-kv store k
;; delete-kvs store ks
;; get store k
;; get-range store prefix-k
;; get-range store prefix-k limit
;; seek ? is essentially supported by get-range
;; count store prefix-k

(defonce my-conn-pool   (car/connection-pool {})) ; Create a new stateful pool
(def     my-conn-spec-1 {:uri "redis://localhost:6379/"})

(def wcar-opts
  {:pool my-conn-pool
   :spec my-conn-spec-1})

(defn ->buffer [v] (nippy/freeze v))
(defn ->value [b] (nippy/thaw b))


;; TODO replace :store with something like configurable :eav
(defn set-k [conn keyspace k]
  (wcar conn (car/zadd keyspace 0 (car/raw k))))

(defn set-ks [conn keyspace ks]
  (wcar conn (apply car/zadd keyspace (mapcat #(vector 0 (car/raw %)) ks))))

(defn delete-k [conn keyspace k]
  (wcar conn (car/zrem keyspace 0 (car/raw k))))

(defn delete-ks [conn keyspace ks]
  (wcar conn (apply car/zrem keyspace (map #(vector 0 (car/raw %)) ks))))

(defn get-k [conn keyspace k]
  (when (wcar conn (car/zscore keyspace (car/raw k))) k))

(def ^:private inclusive-byte (int \[))
(def ^:private exclusive-byte (int \())

(defn- inclusive-key [k]
  (byte-array (mapcat seq [(byte-array [inclusive-byte]) k])))

(defn- exclusive-key [k]
  (byte-array (mapcat seq [(byte-array [exclusive-byte]) k])))

(defn get-range
  ([conn keyspace start-k stop-k]
   (wcar conn
         (->
          (car/zrangebylex keyspace (car/raw (inclusive-key start-k)) (car/raw (exclusive-key stop-k)))
          car/parse-raw)))
  ([conn keyspace start-k stop-k limit]
   (wcar conn
         (->
          (car/zrangebylex keyspace (car/raw (inclusive-key start-k)) (car/raw (exclusive-key stop-k))
                           :limit 0 limit)
          car/parse-raw))))

(defn seek
  ([conn keyspace prefix-k]
   (wcar conn
         (-> (car/zrange keyspace (car/raw (inclusive-key prefix-k)) "+" "BYLEX")
             car/parse-raw)))
  ([conn keyspace prefix-k limit]
   (wcar conn
         (-> (car/zrange keyspace (car/raw (inclusive-key prefix-k)) "+" "BYLEX"
                         :limit 0 limit)
             car/parse-raw))))

(defn count-ks
  ([conn keyspace] (wcar conn (car/zcard keyspace)))
  ([conn keyspace prefix-k] (wcar conn (car/zlexcount keyspace (car/raw (inclusive-key prefix-k)) "+")))
  ([conn keyspace start-k stop-k]
   (wcar conn (car/zlexcount keyspace (car/raw (inclusive-key start-k)) (car/raw (exclusive-key stop-k))))))

;; ADMIN

(defn clear-set
  "WARNING! This clears the entire keyspace."
  [conn keyspace]
  (wcar conn (car/del keyspace)))

;; TODO handle return value
(defn clear-db
  "WARNING! This clears the entire db."
  [conn]
  (wcar conn (car/flushdb)))

(defn alive? [conn]
  (= "PONG" (wcar conn (car/ping))))

(comment
  (set-k wcar-opts :store (->buffer "foo"))
  (->value (get-k wcar-opts :store (->buffer "foo")))
  (get-k wcar-opts :store (->buffer "random"))
  (delete-k wcar-opts :store (->buffer "foo"))

  (->> (for [i (range 10)]
         (str "foo" i))
       (map ->buffer)
       (set-ks wcar-opts :store))

  (clear-set wcar-opts :store)
  (clear-db wcar-opts)

  ;; fix inclusive/exclusive
  (->> (get-range wcar-opts :store (->buffer "foo") (->buffer "foo2"))
       (map ->value))

  (->> (get-range wcar-opts :store (->buffer "foo") (->buffer "foo9") 5)
       (map ->value))

  (->> (seek wcar-opts :store (->buffer "foo"))
       (map #(try (->value %) (catch Exception e :some-error))))

  (->> (seek wcar-opts :store (->buffer "foo") 5)
       (map #(try (->value %) (catch Exception e :some-error))))

  (->> (count-ks wcar-opts :store))
  (->> (count-ks wcar-opts :store (->buffer "foo2")))
  (->> (count-ks wcar-opts :store (->buffer "foo") (->buffer "foo4")))

  (alive? wcar-opts))

;; INFO parsing

(defn- parse-kv [kv]
  (let [[k v] (str/split kv #":")]
    [(keyword k) v]))

(defn- parse-section [section]
  (let [[section-name & values] (str/split-lines section)]
    [(keyword (str/lower-case section-name)) (into {} (map parse-kv) values)]))

(defn info [conn]
  (let [sections  (-> (wcar conn (car/info)) (str/split #"#"))
        sections (->> sections (remove str/blank?) (map str/trim))]
    (into {} (map parse-section) sections)))

(comment
  (info wcar-opts))
