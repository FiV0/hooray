(ns hooray.db
  (:require [clojure.string :as str]
            [clojure.core.match :refer [match]]))

(defn valid-db-type? [{:keys [type sub-type algo]}]
  (match [type sub-type algo]
    [:mem nil nil] true
    [:mem :core nil] true
    [:mem :core :leapfrog] true
    [:mem :avl nil] true
    [:mem :avl :leapfrog] true
    [:mem :avl :generic] true
    [:mem :tonsky nil] true
    [:mem :tonsky :leapfrog] true
    [:mem :tonsky :generic] true
    :else false))

;; copied from asami
(defn parse-uri
  "Splits up a database URI string into structured parts"
  [uri]
  (if (map? uri)
    uri
    (if-let [[_ db-type sub-type algo db-name] (re-find #"hooray:([^:]+):([^:]*)(:[^:]*)?//(.+)" uri)]
      (let [uri-map {:type (keyword db-type)
                     :sub-type (if (str/blank? sub-type) nil (keyword sub-type))
                     :algo (if (str/blank? algo) nil (keyword (subs algo 1)))
                     :name db-name}]
        (if (valid-db-type? uri-map)
          uri-map
          (throw (ex-info "Database type currently not supported!" {:uri-map uri-map}))))
      (throw (ex-info (str "Invalid URI: " uri) {:uri uri})))))

(comment
  (ns-unmap *ns* 'connect*))

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
  (in-between [this t1 t2] "Retrieves a database with facts added or retrieved in between t1 and t2."))

(defprotocol Connection
  (get-name [this] "Returns the name of the connection.")
  (db [this] "Returns the db of a connection.")
  (transact [this tx-data] "Submits a transaction to the database for writing."))

(defprotocol GraphDatabase
  (graph [this] "Returns the the underlying graph of the database"))
