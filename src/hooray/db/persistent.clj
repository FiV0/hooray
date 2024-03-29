(ns hooray.db.persistent
  (:require [hooray.db :as db]
            [hooray.db.persistent.fdb :as fdb]
            [hooray.db.persistent.graph :as per-g]
            [hooray.db.persistent.packing :as pack]
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
  (get-comp [this] #(cond (nil? %1) -1
                          (nil? %2)  1
                          :else (pack/compare-unsigned %1 %2)))
  (get-hash-fn [this] #(pack/hash->bb (hash %)))

  db/GraphDatabase
  (graph [this] graph))

(defn- latest-or-date? [timestamp]
  (or (= :latest timestamp)
      (instance? java.util.Date timestamp)))

;; :latest is going to have a special meaning in the context of a db
(defn ->persistent-db
  ([conn key-store doc-store]
   (->PersistentDb conn (per-g/->persistent-graph conn key-store doc-store) :lastest {}))
  ([conn timestamp key-store doc-store]
   {:pre [(latest-or-date? timestamp)]}
   (->PersistentDb conn (per-g/->persistent-graph conn key-store doc-store) timestamp {}))
  ([conn timestamp key-store doc-store opts]
   {:pre [(latest-or-date? timestamp)]}
   (->PersistentDb conn (per-g/->persistent-graph conn key-store doc-store) timestamp opts)))

(defrecord PersistentConnection [name connection type key-store doc-store opts]
  db/Connection
  (get-name [this] name)
  (db [this] (->persistent-db connection :latest key-store doc-store opts))
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
      (fdb/fdb-connection? connection) (fdb/clear-db connection)
      :else (throw (ex-info "No such connection type known!" {:conn-type (type connection)})))))

(defn- ->key-store [type conn]
  (case type
    :redis (redis/->redis-key-store conn)
    :fdb (fdb/->fdb-key-store conn)
    (throw (ex-info "No such persistent backing store known!" {:type type}))))

(defn- ->doc-store [type conn]
  (case type
    :redis (redis/->redis-doc-store conn)
    :fdb (fdb/->fdb-doc-store conn)
    (throw (ex-info "No such persistent backing store known!" {:type type}))))

(defn ->persistent-connection [{:keys [sub-type name] :as config-map}]
  (let [conn (proto/config-map->conn config-map)
        key-store (->key-store sub-type conn)
        doc-store (->doc-store sub-type conn)
        opts {:uri-map config-map}]
    (->PersistentConnection name conn sub-type key-store doc-store opts)))

(comment
  ;; "hooray:per:redis:hash//localhost:6379"
  (->persistent-connection {:sub-type :redis
                            :name "hello"
                            :spec {:uri "redis://localhost:6379/"}})
  (->persistent-connection {:sub-type :fdb
                            :name "hello"
                            :spec {}})
  )

;; TODO this whole connection business doesn't properly use multimethods yet
(defmethod db/connect* :per [{:keys [sub-type _algo _name] :as uri-map}]
  (case sub-type
    (:redis :fdb) (->persistent-connection uri-map)
    (throw (ex-info "Persistent connection of this type currently not supported!" uri-map))))
