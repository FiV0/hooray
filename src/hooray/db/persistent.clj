(ns hooray.db.persistent
  (:require [hooray.db :as db]
            [hooray.db.persistent.fdb :as fdb]
            [hooray.db.persistent.protocols :as proto]
            [hooray.db.persistent.redis :as redis]
            [hooray.db.persistent.transact :as transact]
            [hooray.util :as util])
  (:import (java.io Closeable)))


(defrecord PersistentDb [connection timestamp key-store doc-store]
  db/Database
  (as-of [this t] (util/unsupported-ex))
  (as-of-t [this] timestamp)
  (entity [this eid] (util/unsupported-ex)))

(defn- latest-or-date? [timestamp]
  (or (= :lastest timestamp)
      (instance? java.util.Date timestamp)))

;; :latest is going to have a special meaning in the context of a db
(defn ->persistent-db
  ([conn key-store doc-store]
   (->PersistentDb conn :lastest key-store doc-store))
  ([conn timestamp key-store doc-store]
   {:pre [(latest-or-date? timestamp)]}
   (->PersistentDb conn :lastest key-store doc-store)))

(defrecord PersistentConnection [name connection type key-store doc-store]
  db/Connection
  (get-name [this] name)
  (db [this] (->persistent-db connection :latest key-store doc-store))
  (transact [this tx-data] (transact/transact connection tx-data))

  Closeable
  (close [_]
    (cond
      (redis/redis-connection? connection) (redis/close-connection connection)
      (fdb/fdb-connection? connection) (fdb/close-connection connection)
      :else (throw (ex-info "No such connection type known!" {:conn-type (type connection)}))))

  db/DropDB
  (drop-db [this]
    (cond
      (redis/redis-connection? connection) (redis/clear-db connection)
      (fdb/fdb-connection? connection) (fdb/close-connection connection)
      :else (throw (ex-info "No such connection type known!" {:conn-type (type connection)})))))

(defn- ->key-store [type conn]
  (case type
    :redis (redis/->redis-key-store conn)
    :fdb (throw (ex-info "FDB KeyStore not yet implemented!" {}))
    (throw (ex-info "No such persistent backing store known!" {:type type}))))

(defn- ->doc-store [type conn]
  (case type
    :redis (redis/->redis-doc-store conn)
    :fdb (throw (ex-info "FDB DocStore not yet implemented!" {}))
    (throw (ex-info "No such persistent backing store known!" {:type type}))))

(defn ->persistent-connection [{:keys [type name] :as config-map}]
  (let [conn (proto/config-map->conn config-map)
        key-store (->key-store type conn)
        doc-store (->doc-store type conn)]
    (->PersistentConnection name conn type key-store doc-store)))

(comment
  (->persistent-connection {:type :redis
                            :name "hello"
                            :spec {:uri "redis://localhost:6379/"}}))

;; TODO this whole connection business doesn't properly use multimethods yet
(defmethod db/connect* :persistent [uri-map]
  (throw (ex-info "Persistent connections are currently not supported!" uri-map)))
