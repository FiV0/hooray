(ns hooray.db.persistent
  (:require [hooray.db :as db]
            [hooray.util :as util])
  (:import (java.io Closeable)))

(defmethod db/connect* :persistent [uri-map]
  (throw (ex-info "Persistent connections are currently not supported!" uri-map)))

;; The idea is to have either a connection string or a config-map
;; Redis as an example
(comment
  {:type :redis
   :spec {:uri "redis://localhost:6379/"}})

(defmulti config-map->conn (fn [config-map] (:type config-map)))

(defmethod config-map->conn :default [config-map]
  (throw (ex-info "Unkown connection type!" {:config-map config-map})))

;; API an persistent key/value store should support
;; set-k store k
;; set-ks store ks
;; delete-k store k
;; delete-ks store ks
;; upsert-ks store ks - combination of set and delete
;; get store k
;; get-range store prefix-k
;; get-range store prefix-k limit
;; seek ? is essentially supported by get-range
;; count store prefix-k

;; DOC STORE
;; set-kv store k v
;; set-kvs store kvs
;; get-kv store k -> v
;; get-kvs store ks -> vs
;; delete-kv store k
;; delete-kvs store kvs
;; upsert-kvs store kvs - combination of set and delete

;; TODO should this be named HashStore?
(defprotocol KeyStore
  (set-k [this keyspace k])
  (set-ks [this keyspace ks])
  (delete-k [this keyspace k])
  (delete-ks [this keyspace ks])
  (upsert-ks [this ops])
  (get-k [this keyspace k])
  (get-range
    [this keyspace begin end]
    [this keyspace begin end limit])
  (seek
    [this keyspace prefix-k]
    [this keyspace prefix-k limit])
  (count-ks
    [this keyspace]
    [this keyspace prefix-k]
    [this keyspace begin end]))

(defprotocol DocStore
  (set-kv [this keyspace k v])
  (set-kvs [this keyspace kvs])
  (get-kv [this keyspace k])
  (get-kvs [this keyspace ks])
  (delete-kv [this keyspace k])
  (delete-kvs [this keyspace ks])
  (upsert-kvs [this ops]))

(defrecord PersistentDb [connection timestamp key-store doc-store]
  db/Database
  (as-of [this t] (util/unsupported-ex))
  (as-of-t [this] timestamp)
  (entity [this eid] (util/unsupported-ex)))

(defrecord PersistentConnection [name connection type]
  db/Connection
  (get-name [this] name)
  (db [this] (util/unsupported-ex))
  (transact [this tx-data] (util/unsupported-ex))

  Closeable
  (close [_]))
