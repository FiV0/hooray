(ns hooray.db.persistent.redis
  (:require [clojure.string :as str]
            [taoensso.carmine :as car :refer [wcar]]
            [taoensso.nippy :as nippy]))

(defonce my-conn-pool   (car/connection-pool {})) ; Create a new stateful pool
(def     my-conn-spec-1 {:uri "redis://localhost:6379/"})

(def wcar-opts
  {:pool my-conn-pool
   :spec my-conn-spec-1})

(defn ->buffer [v] (nippy/freeze v))
(defn ->value [b] (nippy/thaw b))

(defn set-k [conn keyspace k]
  (wcar conn (car/zadd keyspace 0 (car/raw k))))

(defn set-ks [conn keyspace ks]
  (wcar conn (apply car/zadd keyspace (mapcat #(vector 0 (car/raw %)) ks))))

(defn delete-k [conn keyspace k]
  (wcar conn (car/zrem keyspace 0 (car/raw k))))

(defn delete-ks [conn keyspace ks]
  (wcar conn (apply car/zrem keyspace (mapcat #(vector 0 (car/raw %)) ks))))

(defn- third [c] (nth c 2))
(def ^:private seperate (juxt filter remove))

(defn upsert-ks
  "The ks are [keyspace k op] pairs. Anything truthy will be considered an assert."
  [conn ks]
  (wcar conn (car/multi))
  (let [key-fn #(map second %)
        [asserts deletes] (->> (seperate third ks)
                               (map (comp #(update-vals % key-fn) #(group-by first %))))]
    ;; [asserts deletes]
    (run! (fn [[keyspace ks]] (set-ks conn keyspace ks)) asserts)
    (run! (fn [[keyspace ks]] (delete-ks conn keyspace ks)) deletes))
  (wcar conn (car/exec)))

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

(comment
  (set-k wcar-opts :store (->buffer "foo"))
  (->value (get-k wcar-opts :store (->buffer "foo")))
  (get-k wcar-opts :store (->buffer "random"))
  (delete-k wcar-opts :store (->buffer "foo"))
  (delete-ks wcar-opts :store [(->buffer "foo")])

  (upsert-ks wcar-opts [[:store (->buffer "foo") nil] [:store (->buffer "foo1") true] [:store1 (->buffer "foo") true]])
  (->value (get-k wcar-opts :store (->buffer "foo")))


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
  (->> (count-ks wcar-opts :store (->buffer "foo") (->buffer "foo4"))))

;; DOC STORE

(defn set-kv [conn keyspace k v]
  (wcar conn (car/hset keyspace (car/raw k) (car/raw v))))

(defn set-kvs [conn keyspace kvs]
  (wcar conn (apply car/hset keyspace (mapcat (fn [[k v]] [(car/raw k) (car/raw v)]) kvs))))

(defn get-kv [conn keyspace k]
  (wcar conn (-> (car/hget keyspace (car/raw k)) car/parse-raw)))

(defn get-kvs [conn keyspace ks]
  (wcar conn (-> (apply car/hmget keyspace (map car/raw ks)) car/parse-raw)))

(defn delete-kv [conn keyspace k]
  (wcar conn (car/hdel keyspace (car/raw k))))

(defn delete-kvs [conn keyspace ks]
  (wcar conn (apply car/hdel keyspace (map car/raw ks))))

(defn upsert-kvs
  "The kvs are [keyspace kv op] pairs. Anything truthy will be considered an assert.
  In case of delete kv should just be a single a key."
  [conn ks]
  (wcar conn (car/multi))
  (let [key-fn #(map second %)
        [asserts deletes] (->> (seperate third ks)
                               (map (comp #(update-vals % key-fn) #(group-by first %))))]
    (run! (fn [[keyspace kvs]] (set-kvs conn keyspace kvs)) asserts)
    (run! (fn [[keyspace ks]] (delete-kvs conn keyspace ks)) deletes))
  (wcar conn (car/exec)))

(comment
  (set-kv wcar-opts :doc-store (->buffer "foo") (->buffer "bar"))
  (->value (get-kv wcar-opts :doc-store (->buffer "foo")))
  (set-kvs wcar-opts :doc-store [[(->buffer "foo") (->buffer "bar")] [(->buffer "foo0") (->buffer "bar0")]])
  (->> (get-kvs wcar-opts :doc-store [(->buffer "foo") (->buffer "foo0")])
       (map ->value))
  (delete-kv wcar-opts :doc-store (->buffer "foo0"))
  (get-kv wcar-opts :doc-store (->buffer "foo0"))
  (delete-kvs wcar-opts :doc-store [(->buffer "foo") (->buffer "foo0")])
  (get-kv wcar-opts :doc-store (->buffer "foo"))
  (upsert-kvs wcar-opts [[:doc-store (->buffer "foo") nil]
                         [:doc-store [(->buffer "foo1") (->buffer "bar")] true]
                         [:doc-store1 [(->buffer "foo") (->buffer "bar")] true]])
  (get-kv wcar-opts :doc-store (->buffer "foo"))
  (->value (get-kv wcar-opts :doc-store (->buffer "foo1")))
  (->value (get-kv wcar-opts :doc-store1 (->buffer "foo"))))

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
  (clear-db wcar-opts)
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
