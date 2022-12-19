(ns hooray.db.persistent
  (:require [hooray.db :as db]
            [hooray.util :as util]
            [hooray.db.persistent.protocols :as proto]
            [hooray.db.persistent.redis :as redis]
            [hooray.db.persistent.fdb :as fdb])
  (:import (java.io Closeable)))

(defmethod db/connect* :persistent [uri-map]
  (throw (ex-info "Persistent connections are currently not supported!" uri-map)))

(defrecord PersistentDb [connection timestamp key-store doc-store]
  db/Database
  (as-of [this t] (util/unsupported-ex))
  (as-of-t [this] timestamp)
  (entity [this eid] (util/unsupported-ex)))

(declare transact*)

(defrecord PersistentConnection [name connection type]
  db/Connection
  (get-name [this] name)
  (db [this] (util/unsupported-ex))
  (transact [this tx-data] (util/unsupported-ex))

  Closeable
  (close [_]))
