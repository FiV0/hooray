(ns hooray.db.memory
  (:require [clojure.spec.alpha :as s]
            [hooray.datom :as datom :refer [->Datom]]
            [hooray.db :as db]
            [hooray.db.memory.graph :as graph]
            [hooray.db.memory.bitemp-graph :as bi-graph]
            [hooray.util :as util])
  (:import (java.io Closeable)))


(defrecord MemoryDatabase [graph history timestamp]
  db/Database
  (as-of [this t] (throw (ex-info "todo" {})))
  (as-of-t [this] timestamp)
  (entity [this eid] (graph/entity graph eid))

  db/GraphDatabase
  (graph [this] graph))

(declare transact*)

(defrecord MemoryConnection [name state type]
  db/Connection
  (get-name [this] name)
  (db [this] (-> this :state deref :db))
  (transact [this tx-data] (transact* this tx-data))

  Closeable
  (close [_] ;; a no-op for memory databases
    nil))

(defmethod db/connect* :mem [{:keys [name] :as _uri-map}]
  (let [db (->MemoryDatabase (graph/memory-graph) [] (util/now))]
    (->MemoryConnection name (atom {:db db}) :mem)))

(defmethod db/connect* :bi-mem [{:keys [name] :as _uri-map}]
  (let [db (->MemoryDatabase (bi-graph/memory-bitemp-graph)[] (util/now))]
    (->MemoryConnection name (atom {:db db}) :bi-mem)))

(s/def :hooray/map-transaction map?)
(s/def :hooray/add-transaction #(and (= :db/add (first %)) (vector? %) (= 4 (count %))))
(s/def :hooray/retract-transaction #(and (= :db/retract (first %)) (vector? %) (= 4 (count %))))
(s/def :hooray/transaction (s/or :map :hooray/map-transaction
                                 :add :hooray/add-transaction
                                 :retract :hooray/retract-transaction))
(s/def :hooray/tx-data (s/* :hooray/transaction))

(comment
  (s/valid? :hooray/tx-data [{:db/id "foo"
                              :foo/bar "x"}
                             [:db/add "foo" :is/cool true]]))

(defn- map->datoms [m ts]
  (let [eid (or (:db/id m) (random-uuid))]
    (->> (dissoc m :db/id)
         (map (fn [[k v]] (->Datom eid k v ts true))))))

(defn transaction->datoms [transaction ts]
  (cond
    (map? transaction) (map->datoms transaction ts)
    (= :db/add (first transaction)) [(apply ->Datom (concat (rest transaction) [ts true]))]
    (= :db/retract (first transaction)) [(apply ->Datom (concat (rest transaction) [ts false]))]))

(defn transact* [{:keys [state type] :as _connection} tx-data]
  (case type
    :mem (s/assert :hooray/tx-data tx-data)
    :bi-mem nil
    (throw (ex-info "No such connection type!" {:type type})))
  (let [ts (util/now)
        ;; datoms (mapcat #(transaction->datoms % ts) tx-data)
        [{db-before :db} {db-after :db}]
        (swap-vals! state
                    (fn [{db-before :db}]
                      (let [{:keys [graph history]} db-before
                            new-graph (graph/transact graph tx-data ts)]
                        {:db (->MemoryDatabase new-graph (conj history db-before) ts)})))]
    {:db-before db-before
     :db-after db-after
     ;; :tx-data datoms
     }))

(comment
  (def conn (db/connect "hooray:mem://data"))
  (db/db conn)

  (def data [{:db/id "foo"
              :foo/bar "x"}
             [:db/add "foo" :is/cool true]])

  (db/transact conn data)

  (-> conn db/db (db/entity "foo"))


  )
