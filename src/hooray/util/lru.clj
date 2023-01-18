(ns hooray.util.lru
  "A thin wrapper around `clojure.core.cached`"
  (:refer-clojure :exclude [get])
  (:require [clojure.core.cache.wrapped :as cw]))

(defprotocol Cache
  (get [this item]))

(deftype LruCache [cache hash-fn]
  Cache
  (get [_this item]
    (cw/lookup-or-miss cache item hash-fn))

  clojure.lang.Counted
  (count [_this]
    (count @cache)))

(defn create-lru-cache
  ([threshold hash-fn] (create-lru-cache {} threshold hash-fn))
  ([data threshold hash-fn]
   (->LruCache (cw/lru-cache-factory (into {} (map #(vector (hash-fn %) %)) data) :threshold threshold)
               hash-fn)))

(comment
  (def cache (create-lru-cache {} 2 #(do (println "calc") (hash %))))
  (get cache "foo")
  (get cache "bar")
  (get cache "toto"))
