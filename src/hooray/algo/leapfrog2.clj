(ns hooray.algo.leapfrog2
  (:require [clojure.spec.alpha :as s]
            [hooray.db :as db]
            [hooray.db.memory.graph-index :as g-index]
            [hooray.graph :as graph]
            [hooray.query.spec :as h-spec]
            [hooray.util :as util]
            [medley.core :refer [map-kv]]))

(comment
  (def q '{:find [?name ?album]
           :where [[?t :track/name "For Those About To Rock (We Salute You)" ]
                   [?t :track/album ?album]
                   [?album :album/artist ?artist]
                   [?artist :artist/name ?name]]
           ;; :keys [name album]
           })

  (def q-conformed (s/conform ::h-spec/query q)))

(defn- logic-var? [[type _value]]
  (= :logic-var type))

(defn- ->value [[_ value]] value)

(defn- var->indices [where]
  (->> (map-indexed #(vector %1 (second %2)) where)
       (reduce (fn [res [i {:keys [e a v]}]]
                 (cond-> res
                   (logic-var? e) (update (->value e) (fnil conj []) i)
                   (logic-var? a) (update (->value a) (fnil conj []) i)
                   (logic-var? v) (update (->value v) (fnil conj []) i))) {})))

(comment
  (var->indices (:where q-conformed)))

(defn- triple->vars [{:keys [e a v]}]
  (reduce #(if (logic-var? %2) (conj %1 (second %2)) %1) [] [e a v]))

(defn triple->sorted-vars [triple var->join-index]
  (->> (triple->vars triple)
       (sort-by var->join-index)))

(defn- triple+var->level-map [where var->join-index]
  (->> (map-indexed #(vector %1 (second %2)) where)
       (reduce (fn [res [i triple]]
                 (->> (map-indexed vector (triple->sorted-vars triple var->join-index))
                      (reduce (fn [res [level var]] (assoc res [i var] level)) res))) {})))

(defn- var->join-index [var-join-order]
  (zipmap var-join-order (range)))

(comment
  (def vars (keys (var->indices(:where q-conformed))))
  (def var->j-index (var->join-index vars))
  (triple+var->level-map (:where q-conformed) var->j-index))

(defn- var->bookkeeping-map [where var->join-index]
  (let [var->indices (var->indices where)
        triple+var->level-map (triple+var->level-map where var->join-index)]
    (map-kv (fn [var indices]
              [var {:pos 0
                    :indices indices
                    :non-top-indices (vec (remove #(= 0 (triple+var->level-map [% var]))indices))}])
            var->indices)))

(comment
  (var->bookkeeping-map (:where q-conformed) var->j-index))

(defn triple->tuple
  ([triple] {:triple triple :triple-order [:e :a :v]})
  ([{:keys [e a v] :as triple} var->join-index]
   (let [[literals vars] (->> (map vector [e a v] [:e :a :v])
                              ((juxt remove filter) (comp logic-var? first)))
         vars (sort-by #(-> %1 first second var->join-index) vars)
         triple-order (into (vec (map second literals)) (map second vars))]
     {:triple triple
      :triple-order triple-order})))

(comment
  (triple->tuple (-> q-conformed :where first second) var->j-index)
  (triple->tuple (-> q-conformed :where second second) var->j-index)
  (triple->tuple (-> q-conformed :where (nth 2) second) var->j-index))

(defn- sort-indices [indices iterators]
  (let [keyfn (zipmap indices (map g-index/key iterators))]
    (vec (sort-by keyfn indices))))

(s/def ::ids->iterators (s/map-of int? g-index/leap-iterator?))
(s/def ::pos int?)
(s/def ::indices (s/and vector? (s/* int?)))
(s/def ::non-top-indices (s/and vector? (s/* int?)))
(s/def ::bookkeeping (s/keys :req-un [::pos ::indices ::non-top-indices]))
(s/def ::var->bookkeeping (s/map-of ::h-spec/logic-var ::bookkeeping))

;; TODO maybe try a stack approach here
(s/fdef leapfrog-up :args (s/cat :var ::h-spec/logic-var :var->bookkeeping ::var->bookkeeping :ids->iterators ::ids->iterators))

(defn leapfrog-up [var var->bookkeeping ids->iterators]
  (let [indices (get-in var->bookkeeping [var :non-top-indices])]
    (reduce (fn [ids->iterators i] (update ids->iterators i #(g-index/up %))) ids->iterators indices)))

(s/fdef leapfrog-down :args (s/cat :var ::h-spec/logic-var :var->bookkeeping ::var->bookkeeping :ids->iterators ::ids->iterators))

(defn leapfrog-down [var var->bookkeeping ids->iterators]
  (let [indices (get-in var->bookkeeping [var :indices])
        non-top-indices (get-in var->bookkeeping [var :non-top-indices])
        ids->iterators (reduce (fn [ids->iterators i] (update ids->iterators i #(g-index/open %))) ids->iterators non-top-indices)]
    [(-> var->bookkeeping
         (assoc-in [var :pos] 0)
         (assoc-in [var :indices] (sort-indices indices (map ids->iterators indices))))
     ids->iterators]))

(defn- iterator-key [iterators p]
  (g-index/key (nth iterators p)))

(defn- iterator-next [iterators p]
  {:pre [(vector? iterators)]}
  (update iterators p g-index/next))

(defn- iterator-seek [iterators p x]
  {:pre [(vector? iterators)]}
  (update iterators p #(g-index/seek % x)))

(defn- iterator-end? [iterators p]
  (g-index/at-end? (nth iterators p)))

(s/fdef leapfrog-next :args (s/cat :var ::h-spec/logic-var :var->bookkeeping ::var->bookkeeping :ids->iterators ::ids->iterators))

(defn leapfrog-next
  "Returns a tuple of next-value, next-iterator-pos and updated idx->iterators.
  next-value will be nil if no value could be found."
  [var var-bookkeeping idx->iterators]
  (let [var-pos (get-in var-bookkeeping [var :pos])
        indices (get-in var-bookkeeping [var :indices])
        k (count indices)
        iterators (mapv idx->iterators indices)]
    (loop [p var-pos itrs iterators]
      (let [x' (iterator-key itrs (mod (dec p) k))
            x (iterator-key itrs p)]
        (cond
          (or (nil? x) (nil? x))
          [nil nil (->> itrs (zipmap indices) (into idx->iterators))]

          (= x' x)
          [x
           (assoc-in var-bookkeeping [var :pos] (inc p))
           (->> (iterator-next itrs p) (zipmap indices) (into idx->iterators))]

          :else
          (let [itrs (iterator-seek itrs p x')]
            (if (iterator-end? itrs p)
              [nil nil (->> itrs (zipmap indices) (into idx->iterators))]
              (recur (mod (inc p) k) itrs))))))))

(defn- lookup-row [graph row]
  (mapv #(g-index/hash->value graph %) row))

(defn- pop-empty [v]
  (if (seq v) (pop v) nil))

;; var-bookkeeping is a map from var to
;; {:pos             - the current position of the iterators
;;  :indices         - all the iterator indices involved for this var
;;  :non-top-indices - all indices of iterators that are not
;; }

;; lazy-seq ?
(defn join [{:keys [query var-join-order var->bindings] :as _compiled-q} db]
  {:pre [(vector? var-join-order)]}
  (if-let [where (:where query)]
    (let [max-level (count var-join-order)
          graph (db/graph db)
          tuples (->> (filter (comp #{:triple} first) where)
                      (mapv (comp #(triple->tuple % var->bindings) second)))
          idx->iterators (->> (map #(graph/get-iterator graph %) tuples)
                              (zipmap (range max-level)))
          ;; we add a dummy var to bottom out, simplifies the code below
          dummy-var (gensym "?dummy-var")
          var-join-order (conj var-join-order dummy-var)
          var-join-index (var->join-index var-join-order)
          first-var (first var-join-order)
          var-bookkeeping (-> (var->bookkeeping-map where var-join-index)
                              (update-in [first-var :indices] #(sort-indices % (map idx->iterators %)))
                              (assoc dummy-var {:pos 0 :indices [] :non-top-indices []}))]
      (loop [res nil partial-row [] var-level 0
             var-bookkeeping var-bookkeeping idx->iterators idx->iterators]
        (cond

          ;; finished
          (neg? var-level) (map (partial lookup-row graph) res)

          ;; bottomed out
          (= var-level max-level)
          (recur (cons partial-row res) (pop-empty partial-row) (dec var-level)
                 var-bookkeeping idx->iterators)

          :else
          (let [var (nth var-join-order var-level)
                [val var-bookkeeping idx->iterators]
                (leapfrog-next var var-bookkeeping idx->iterators)]
            (println [var var-bookkeeping idx->iterators])
            (if val
              (let [var (nth var-join-order (inc var-level))
                    [var-bookkeeping idx->iterators] (leapfrog-down var var-bookkeeping idx->iterators)]
                (recur res (conj partial-row val) (inc var-level)
                       var-bookkeeping idx->iterators))
              (recur res (pop-empty partial-row) (dec var-level)
                     var-bookkeeping (leapfrog-up var var-bookkeeping idx->iterators)))))))
    (throw (ex-info "Query must contain where clause!" {:query query}))))


(comment
  (require '[clojure.edn :as edn]
           '[hooray.query :as query])
  (def data (doall (edn/read-string (slurp "resources/transactions.edn"))))

  (def conn (db/connect "hooray:mem:avl//data"))
  (db/transact conn data)
  (db/transact conn [[:db/add 1 2 3] [:db/add 4 5 6]])
  (defn get-db [] (db/db conn))

  (def q '{:find [?name ?album]
           :where [[?t :track/name "For Those About To Rock (We Salute You)" ]
                   [?t :track/album ?album]
                   [?album :album/artist ?artist]
                   [?artist :artist/name ?name]]
           ;; :keys [name album]
           })

  (do
    (def q '{:find [?first ?last]
             :where [[?first 5 ?last]]})

    (def q-conformed (s/conform ::h-spec/query q))
    (def compiled-q (-> (query/compile-query2 q (get-db))
                        (assoc :query q-conformed)))

    )

  (join compiled-q (get-db))

  (def tupels
    (->> (-> compiled-q :query :where)
         (filter (comp #{:triple} first))
         (mapv (comp #(triple->tuple % (:var->bindings compiled-q)) second))))

  (defn get-iterator [tuple]
    (graph/get-iterator (db/graph (get-db)) tuple))


  (def it (get-iterator (first tupels) ))
  (get-iterator (second tupels) )
  (get-iterator (nth tupels 2) )
  (get-iterator (nth tupels 3) )

  (-> it g-index/key )
  (-> it g-index/at-end? )
  (-> it g-index/next g-index/at-end?)
  )
