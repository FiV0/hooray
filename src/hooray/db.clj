(ns hooray.db
  (:require [clojure.core.match :refer [match]]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]))

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

;; redis
(s/def :redis/uri string?)
(s/def :redis/spec (s/keys :req-un [:redis/uri]))
(s/def :redis/config (s/keys :req-un [:redis/spec]))

;; fdb
(s/def :fdb/cluster-file string?)
(s/def :fdb/spec (s/keys :req-un [:fdb/cluster-file]))
(s/def :fdb/config (s/keys :req-un [:fdb/spec]))

(defn- valid-per-uri-map? [{:keys [sub-type] :as uri-map}]
  (case sub-type
    :redis (s/valid? :redis/config uri-map)
    :fdb (s/valid? :fdb/config uri-map)
    true))

;; general
(s/def ::name string?)
(s/def ::algo (s/nilable #{:hash :leapfrog :generic}))
(s/def ::sub-type (s/nilable #{:core :avl :tonsky :redis :fdb}))
(s/def ::type #{:mem :per})
(s/def ::uri-map (s/and (s/keys :req-un [::type ::sub-type ::algo ::name])
                        valid-per-uri-map?))

(comment
  (s/valid? ::uri-map {:type :mem, :sub-type nil, :algo nil, :name "data"})
  (s/valid? ::uri-map {:type :mem
                       :sub-type :avl
                       :name "hello"
                       :algo :leapfrog})

  (s/valid? ::uri-map {:type :per
                       :sub-type :redis
                       :name "hello"
                       :algo :hash
                       :spec {:uri "redis://localhost:6379/"}}))

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
        (if (and (valid-db-type? uri-map) (s/valid? ::uri-map uri-map))
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
  (entity [this id] "Returns an entity for an identifier")
  (get-comp [this] "Returns the key comparator for this database.")
  (get-hash-fn [this] "Returns the key hash-fn."))

(defprotocol BitempDatabase
  (in-between [this t1 t2] "Retrieves a database with facts added or retrieved in between t1 and t2."))

(defprotocol Connection
  (get-name [this] "Returns the name of the connection.")
  (db [this] "Returns the db of a connection.")
  (transact [this tx-data] "Submits a transaction to the database for writing."))

;; TODO convenience for now, drop later or move to other protocol
(defprotocol DropDB
  (drop-db [this] "WARNING! Clears the db."))

(defprotocol GraphDatabase
  (graph [this] "Returns the the underlying graph of the database"))
