(ns hooray.db
  (:require [clojure.string :as str]))

;; copied from asami
(defn parse-uri
  "Splits up a database URI string into structured parts"
  [uri]
  (if (map? uri)
    uri
    (if-let [[_ db-type db-name] (re-find #"hooray:([^:]+)://(.+)" uri)]
      {:type (keyword db-type)
       :name db-name}
      (throw (ex-info (str "Invalid URI: " uri) {:uri uri})))))

(defmulti connect* (fn [{:keys [type]}] type))
(defmethod connect* :default [uri-map]
  (throw (ex-info "No such db implementation" uri-map)))

(defn connect [uri]
  (connect* (parse-uri uri)))

;; mostly copied from datomic.api
(defprotocol Database
  (as-of [this t] "Retrieves a database as of a given moment, inclusive")
  (as-of-t [this] "Returns the as-of point, or nil if none")
  (entity [this id] "Returns an entity for an identifier"))

(defprotocol BitempDatabase
  (in-between [this t1 t2] "Retrieves a database with facts added or retrieved in between t1 and t2.")
  (entity-history [this id] "Retrieves all versions of a document."))

(defprotocol Connection
  (get-name [this] "Returns the name of the connection.")
  (db [this] "Returns the db of a connection.")
  (transact [this tx-data] "Submits a transaction to the database for writing."))

(defprotocol GraphDatabase
  (graph [this] "Returns the the underlying graph of the database"))
