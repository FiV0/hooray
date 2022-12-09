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

(comment
  (int \()
  ;; => 40
  (int \[)
  ;; => 91
  )

(defn- inclusive-key [k]
  (byte-array (mapcat seq [(byte-array [91]) k])))

(defn- exclusive-key [k]
  (byte-array (mapcat seq [(byte-array [40]) k])))

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


(comment
  (set-k my-wcar-opts (->buffer "foo"))
  (->value (get-k my-wcar-opts (->buffer "foo")))
  (get-k my-wcar-opts (->buffer "randomshit"))
  (delete-k my-wcar-opts (->buffer "foo"))

  (->> (for [i (range 10)]
         (str "foo" i))
       (map ->buffer)
       (set-ks my-wcar-opts))

  ;; fix inclusive/exclusive
  (->> (get-range my-wcar-opts (->buffer "foo") (->buffer "foo2"))
       (map ->value))

  (->> (get-range my-wcar-opts (->buffer "foo") (->buffer "foo9") 5)
       (map ->value))


  (->> (seek my-wcar-opts (->buffer "foo"))
       (map #(try (->value %) (catch Exception e :some-error))))

  (->> (seek my-wcar-opts (->buffer "foo") 5)
       (map #(try (->value %) (catch Exception e :some-error))))

  (wcar my-wcar-opts (car/ping))
  (wcar my-wcar-opts (car/zscore :store "dsafadfsa"))

  (wcar my-wcar-opts (car/zadd :store 0 "foo" 0 "bar"))
  (wcar my-wcar-opts (car/zadd :store 0 "foo1"))
  (wcar my-wcar-opts (car/zadd :store 0 "foo2"))
  (wcar my-wcar-opts (car/zadd :store 12 "foo3"))

  (wcar my-wcar-opts (car/zrange :store 0 1))
  (wcar my-wcar-opts (car/zrange :store 0 1 "WITHSCORES"))
  (wcar my-wcar-opts (car/zrange :store "(foo" "[foo1" "BYLEX"))
  (wcar my-wcar-opts (car/zrange :store "(foo" "+" "BYLEX"))



  (wcar my-wcar-opts (car/zrange :store "(foo" "foo2" "BYLEX"))
  (wcar my-wcar-opts (car/zrangebylex :store "foo" "foo2"))
  (wcar my-wcar-opts (car/zrangebylex :store "foo" "foo2"))
  )
