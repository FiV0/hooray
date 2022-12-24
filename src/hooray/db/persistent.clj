(ns hooray.db.persistent
  (:require [hooray.db :as db]
            [hooray.db.persistent.fdb :as fdb]
            [hooray.db.persistent.graph :as per-g]
            [hooray.db.persistent.protocols :as proto]
            [hooray.db.persistent.redis :as redis]
            [hooray.db.persistent.transact :as transact]
            [hooray.util :as util])
  (:import (java.io Closeable)))

(defrecord PersistentDb [connection graph timestamp opts]
  db/Database
  (as-of [this t] (util/unsupported-ex))
  (as-of-t [this] timestamp)
  (entity [this eid] (util/unsupported-ex))

  db/GraphDatabase
  (graph [this] graph))

(defn- latest-or-date? [timestamp]
  (or (= :lastest timestamp)
      (instance? java.util.Date timestamp)))

;; :latest is going to have a special meaning in the context of a db
(defn ->persistent-db
  ([conn key-store doc-store]
   (->PersistentDb conn (per-g/->persistent-graph conn key-store doc-store) :lastest {}))
  ([conn timestamp key-store doc-store]
   {:pre [(latest-or-date? timestamp)]}
   (->PersistentDb conn (per-g/->persistent-graph conn key-store doc-store) timestamp {})))

(defrecord PersistentConnection [name connection type key-store doc-store]
  db/Connection
  (get-name [this] name)
  (db [this] (->persistent-db connection :latest key-store doc-store))
  (transact [this tx-data] (transact/transact this tx-data))

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

(defn ->persistent-connection [{:keys [sub-type name] :as config-map}]
  (let [conn (proto/config-map->conn config-map)
        key-store (->key-store sub-type conn)
        doc-store (->doc-store sub-type conn)]
    (->PersistentConnection name conn sub-type key-store doc-store)))

(comment
  ;; "hooray:per:redis:hash//localhost:6379"
  (->persistent-connection {:sub-type :redis
                            :name "hello"
                            :spec {:uri "redis://localhost:6379/"}}))

;; TODO this whole connection business doesn't properly use multimethods yet
(defmethod db/connect* :per [{:keys [sub-type algo _name] :as uri-map}]
  (case [sub-type algo]
    [:redis :hash] (->persistent-connection uri-map)
    (throw (ex-info "Persistent connection of this type currently not supported!" uri-map))))
