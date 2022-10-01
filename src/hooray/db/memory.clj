(ns hooray.db.memory
  (:require [clojure.spec.alpha :as s]
            [hooray.datom :as datom :refer [->Datom]]
            [hooray.db :as db]
            [hooray.graph :as graph]
            [hooray.db.memory.graph :as mem-graph]
            [hooray.db.memory.graph-index :as g-index]
            [hooray.db.memory.bitemp-graph :as bi-graph]
            [hooray.util :as util])
  (:import (java.io Closeable)
           (hooray.db.memory.graph MemoryGraph)
           (hooray.db.memory.graph_index MemoryGraphIndexed)))

(defrecord MemoryDatabase [graph history timestamp]
  db/Database
  (as-of [this t] (throw (ex-info "todo" {})))
  (as-of-t [this] timestamp)
  (entity [this eid]
    (if (instance? MemoryGraph this)
      (mem-graph/entity graph eid)
      (util/unsupported-ex)))

  db/GraphDatabase
  (graph [this] graph))

(defmethod print-method MemoryDatabase [{:keys [graph timestamp] :as db} ^java.io.Writer w]
  (.write w "#MemoryDatabase{")
  (.write w (str "graph=" (type graph) ","))
  (.write w (str "timestamp=" timestamp))
  (.write w "}"))

(declare transact*)

(defrecord MemoryConnection [name state type]
  db/Connection
  (get-name [this] name)
  (db [this] (-> this :state deref :db))
  (transact [this tx-data] (transact* this tx-data))

  Closeable
  (close [_] ;; a no-op for memory databases
    nil))

(defmethod db/connect* :mem [{:keys [name sub-type] :as uri-map}]
  (let [db (case sub-type
             (nil :graph) (->MemoryDatabase (mem-graph/memory-graph) [] (util/now))
             (:avl :core) (->MemoryDatabase (g-index/memory-graph {:type sub-type}) [] (util/now))
             (throw (util/illegal-ex (str "No such db sub-type " sub-type))))]
    (->MemoryConnection name (atom {:db db}) [:mem sub-type])))


(defmethod db/connect* :bi-mem [{:keys [name] :as uri-map}]
  (let [db (->MemoryDatabase (bi-graph/memory-bitemp-graph) [] (util/now))]
    (->MemoryConnection name (atom {:db db}) [:bi-mem])))

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
  (case (first type)
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
  (def conn (db/connect "hooray:mem:core//data"))
  (def conn (db/connect "hooray:mem:avl//data"))

  (def data [{:db/id "foo"
              :foo/bar "x"}
             [:db/add "foo" :is/cool true]])

  (db/transact conn data)

  (-> conn db/db (db/entity "foo"))


  )
