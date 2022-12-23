(ns hooray.db.persistent.transact
  (:require [clojure.math.combinatorics :as combo]
            [clojure.spec.alpha :as s]
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

(def ^:private binary-combos (combo/permuted-combinations [:e :a :v] 2))
(def ^:private ternary-combos (combo/permuted-combinations [:e :a :v] 3))
(defn- keyword-ls->keyword [kws]
  (->> (map name kws) (apply str) keyword))

;; TODO minimize hashing
(defn- pack-hash-binary [m op]
  (map #(vector (keyword-ls->keyword %)
                (->> ((apply juxt %) m)
                     (apply pack/pack-hash-array))
                op)
       binary-combos))

(defn- pack-hash-ternary [m op]
  (map #(vector (keyword-ls->keyword %)
                (->> ((apply juxt %) m)
                     (apply pack/pack-hash-array))
                op)
       ternary-combos))

(defn create-key-store-txs [[e a v _ts op]]
  (let [[he ha hv] (map pack/hash [e a v])
        m {:e he :a ha :v hv}
        [hea haa hva] (map pack/pack-hash-array [he ha hv])]
    (concat [[:e hea op] [:a haa op] [:v hva op]]
            (pack-hash-binary m op)
            (pack-hash-ternary m op))))

(defn create-doc-store-txs [[e a v _ts op]]
  (let [[hea haa hva] (map (comp pack/pack-hash-array pack/hash) [e a v])
        [eb ab vb] (map pack/->buffer [e a v])]
    [[:doc-store hea eb op] [:doc-store haa ab op] [:doc-store hva vb op]]))

(defn transact* [{:keys [key-store doc-store] :as _conn} tx-data]
  (try
    (proto/upsert-ks key-store (mapcat create-key-store-txs tx-data))
    (proto/upsert-kvs doc-store (mapcat create-doc-store-txs tx-data))
    (catch Exception e
      (throw (ex-info "A transaction error occured!" {:tx-data tx-data} e)))))

(defn transact
  "Takes a PersistentConnection `conn` and `tx-data`."
  [conn tx-data]
  {:pre [(s/assert :hooray/tx-data tx-data)]}
  (let [ts (util/now)
        triples (mapcat #(transaction->triples % ts) tx-data)]
    (transact* conn triples)))

(comment
  (require '[hooray.db.persistent :as per])
  (def conn (per/->persistent-connection {:type :redis
                                          :name "hello"
                                          :spec {:uri "redis://localhost:6379/"}}))

  (def res (transact conn [{:db/id 1 :hello :world}]))

  )
