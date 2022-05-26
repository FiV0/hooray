(ns hooray.db.memory
  (:require [clojure.string :as str]
            [hooray.db :as db]))

(defrecord MemoryDatabase [graph history timestamp]
  db/Database
  (as-of [this t] (throw (ex-info "todo" {})))
  (as-of-t [this] timestamp)
  (entity [this id] (throw (ex-info "todo" {}))))

(defrecord MemoryConnection [name state]
  db/Connection
  (get-name [this] name)
  (db [this] (-> this :state deref :db))
  (transact [this tx-data] (throw (ex-info "todo" {}))))

(defmethod db/connect :mem [uri]
  (let [db ])
  )
