(ns hooray.db.persistent.transact
  (:require [clojure.spec.alpha :as s]
            [hooray.db.persistent.packing :as pack]
            [hooray.db.persistent.protocols :as proto]
            [hooray.txn]
            [hooray.util :as util]))

;; TODO unify with graph_index
(defn- map->triples [m ts]
  (let [eid (or (:db/id m) (random-uuid))]
    (->> (dissoc m :db/id)
         (map (fn [[k v]] (vector eid k v ts true))))))

(defn transaction->triples [transaction ts]
  (cond
    (map? transaction) (map->triples transaction ts)
    (= :db/add (first transaction)) [(vec (concat (rest transaction) [ts true]))]
    (= :db/retract (first transaction)) [(vec (concat (rest transaction) [ts false]))]))

;; indices needed
;; :e :a :v
;; :ea :ev :ae :av :ve :va
;; :eav :eva :aev :ave :vea :vae
;; TODO check if one can iterate cheaply over prefixes


;; TODO do this with combinatoris fn
(defn create-key-store-txs [[e a v _ts op]]
  (let [[he ha hv] (map pack/pack-hash-array [e a v])
        ea (pack/pack-hash-array e a)
        ev (pack/pack-hash-array e v)
        ae (pack/pack-hash-array a e)
        av (pack/pack-hash-array a v)
        ve (pack/pack-hash-array v e)
        va (pack/pack-hash-array v a)
        eav (pack/pack-hash-array e a v)
        eva (pack/pack-hash-array e v a)
        aev (pack/pack-hash-array a e v)
        ave (pack/pack-hash-array a v e)
        vea (pack/pack-hash-array v e a)
        vae (pack/pack-hash-array v a e)]
    [[:e he op] [:a ha op] [:v hv op]]))


(defn transact* [{:keys [key-store doc-store] :as conn} tx-data]

  )

(defn transact
  "Takes a PersistentConnection `conn` and `tx-data`."
  [conn tx-data]
  {:pre [(s/assert :hooray/tx-data tx-data)]}
  (let [ts (util/now)
        triples (mapcat #(transaction->triples % ts) tx-data)]
    (transact* conn triples)))
