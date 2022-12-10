(ns hooray.db.persistent.redis
  (:require [taoensso.carmine :as car :refer [wcar]]
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

(def my-wcar-opts
  {:pool my-conn-pool
   :spec my-conn-spec-1})

(defn ->buffer [v] (nippy/freeze v))
(defn ->value [b] (nippy/thaw b))

(defn alive? [conn]
  (= "PONG" (wcar conn (car/ping))))

;; TODO replace :store with something like configurable :eav
(defn set-k [conn k]
  (wcar conn (car/zadd :store 0 (car/raw k))))

(defn set-ks [conn ks]
  (wcar conn (apply car/zadd :store (mapcat #(vector 0 (car/raw %)) ks))))

(defn delete-k [conn k]
  (wcar conn (car/zrem :store 0 (car/raw k))))

(defn delete-ks [conn ks]
  (wcar conn (apply car/zrem :store (map #(vector 0 (car/raw %)) ks))))

(defn get-k [conn k]
  (when (wcar conn (car/zscore :store (car/raw k))) k))

(def ^:private inclusive-byte (int \[))
(def ^:private exclusive-byte (int \())

(defn- inclusive-key [k]
  (byte-array (mapcat seq [(byte-array [inclusive-byte]) k])))

(defn- exclusive-key [k]
  (byte-array (mapcat seq [(byte-array [exclusive-byte]) k])))

(defn get-range
  ([conn start-k stop-k]
   (wcar conn
         (->
          (car/zrangebylex :store (car/raw (inclusive-key start-k)) (car/raw (exclusive-key stop-k)))
          car/parse-raw)))
  ([conn start-k stop-k limit]
   (wcar conn
         (->
          (car/zrangebylex :store (car/raw (inclusive-key start-k)) (car/raw (exclusive-key stop-k))
                           :limit 0 limit)
          car/parse-raw))))

(defn seek
  ([conn prefix-k]
   (wcar conn
         (-> (car/zrange :store (car/raw (inclusive-key prefix-k)) "+" "BYLEX")
             car/parse-raw)))
  ([conn prefix-k limit]
   (wcar conn
         (-> (car/zrange :store (car/raw (inclusive-key prefix-k)) "+" "BYLEX"
                         :limit 0 limit)
             car/parse-raw))))

(defn count-ks
  ([conn] (wcar conn (car/zcard :store)))
  ([conn prefix-k] (wcar conn (car/zlexcount :store (car/raw (inclusive-key prefix-k)) "+")))
  ([conn start-k stop-k]
   (wcar conn (car/zlexcount :store (car/raw (inclusive-key start-k)) (car/raw (exclusive-key stop-k))))))

(defn clear-set
  "WARNING! This clears the entire keyspace."
  [conn keyspace]
  (wcar conn (car/del keyspace)))


(comment
  (set-k my-wcar-opts (->buffer "foo"))
  (->value (get-k my-wcar-opts (->buffer "foo")))
  (get-k my-wcar-opts (->buffer "randomshit"))
  (delete-k my-wcar-opts (->buffer "foo"))

  (->> (for [i (range 10)]
         (str "foo" i))
       (map ->buffer)
       (set-ks my-wcar-opts))

  (clear-set my-wcar-opts :store)


  ;; fix inclusive/exclusive
  (->> (get-range my-wcar-opts (->buffer "foo") (->buffer "foo2"))
       (map ->value))

  (->> (get-range my-wcar-opts (->buffer "foo") (->buffer "foo9") 5)
       (map ->value))

  (->> (seek my-wcar-opts (->buffer "foo"))
       (map #(try (->value %) (catch Exception e :some-error))))

  (->> (seek my-wcar-opts (->buffer "foo") 5)
       (map #(try (->value %) (catch Exception e :some-error))))

  (->> (count-ks my-wcar-opts))
  (->> (count-ks my-wcar-opts (->buffer "foo0")))
  (->> (count-ks my-wcar-opts (->buffer "foo") (->buffer "foo4")))

  (alive? my-wcar-opts))
