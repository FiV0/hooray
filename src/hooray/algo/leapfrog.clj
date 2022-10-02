(ns hooray.algo.leapfrog
  (:require [clojure.spec.alpha :as s]
            [hooray.db :as db]
            [hooray.db.memory.graph-index :as g-index]
            [hooray.graph :as graph]
            [hooray.query.spec :as h-spec]
            [hooray.util :as util]
            [medley.core :refer [map-kv]]))

;; leapfrog triejoin

;; what do I need ?
;; var -> indices of clause
;; clause + var -> level
;; construction of triples for iterators

;; TODO
;; Multiple variable appearance in triples

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

(defn- var->indices-map [where]
  (->> (map-indexed #(vector %1 (second %2)) where)
       (reduce (fn [res [i {:keys [e a v]}]]
                 (cond-> res
                   (logic-var? e) (update (->value e) (fnil conj []) i)
                   (logic-var? a) (update (->value a) (fnil conj []) i)
                   (logic-var? v) (update (->value v) (fnil conj []) i))) {})))

(comment
  (var->indices-map (:where q-conformed)))

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
  (def vars (keys (var->indices-map (:where q-conformed))))
  (def var->j-index (var->join-index vars))
  (triple+var->level-map (:where q-conformed) var->j-index))

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

(defn- var-indices-sorting [var indices iterators triple-idx+var->level]
  (let [iterators (map #(g-index/set-iterator-level %2 (triple-idx+var->level [var %1])) indices iterators)]
    (sort-indices indices iterators)))

(defn- initial-indices-sorting [var->indices triple-idx+var->level idx->iterators]
  (map-kv (fn [var indices]
            [var (var-indices-sorting var indices (map idx->iterators indices) triple-idx+var->level)])
          var->indices))

(s/def ::ids->iterators (s/map-of int? g-index/leap-iterator?))
(s/def ::var->indices (s/map-of ::h-spec/logic-var (s/and vector? (s/* int?))))

;; TODO maybe try a stack approach here
(s/fdef leapfrog-up :args (s/cat :var ::h-spec/logic-var :var->indices ::var->indices :ids->iterators ::ids->iterators))

(defn leapfrog-up [var var->indices ids->iterators]
  (let [indices (get var->indices var)]
    (reduce (fn [ids->iterators i] (update ids->iterators i #(g-index/up %))) ids->iterators indices)))

(s/fdef leapfrog-down :args (s/cat :var ::h-spec/logic-var :var->indices ::var->indices :ids->iterators ::ids->iterators))

(defn leapfrog-down [var var->indices ids->iterators]
  (let [indices (get var->indices var)]
    (reduce (fn [ids->iterators i] (update ids->iterators i #(g-index/open %))) ids->iterators indices)))

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

(defn leapfrog-next
  "Returns a tuple of next-value, next-iterator-pos and updated idx->iterators.
  next-value will be nil if no value could be found."
  [var var-pos var->indices idx->iterators]
  (let [indices (get var->indices var)
        k (count indices)
        iterators (mapv idx->iterators indices)]
    (loop [p var-pos itrs iterators]
      (let [x' (iterator-key itrs (mod (dec p) k))
            x (iterator-key itrs p)]
        (if (= x' x)
          [x p (->> itrs (zipmap indices) (into idx->iterators))]
          (let [itrs (iterator-seek itrs p x')]
            (if (iterator-end? itrs p)
              [nil nil (->> itrs (zipmap indices) (into idx->iterators))]
              (recur (mod (inc p) k) itrs))))))))

(defn leapfrog-set-next [var var-pos var->indices idx->iterators]
  (let [indices (get var->indices var)
        k (count indices)
        idx (nth indices var-pos)
        iterator (get idx->iterators idx)]
    [(mod (inc var-pos) k) (assoc idx->iterators idx (g-index/next iterator))]))

(defn- lookup-row [graph row]
  (mapv #(g-index/hash->value graph %) row))

(defn- pop-empty [v]
  (if (seq v) (pop v) nil))

(comment
  (require 'sc.api))

;; lazy-seq ?
(defn join [{:keys [query var-join-order var->bindings] :as _compiled-q} db]
  {:pre [(vector? var-join-order)]}
  (if-let [where (:where query)]
    (let [max-level (count var-join-order)
          graph (db/graph db)
          ;; triple-idx+var->level (triple+var->level-map where var->bindings)
          tuples (->> (filter (comp #{:triple} first) where)
                      (mapv (comp #(triple->tuple % var->bindings) second)))
          ;; _ (sc.api/spy)
          idx->iterators (->> (map #(graph/get-iterator graph %) tuples)
                              (zipmap (range max-level)))
          ;; we add a dummy var to bottom out, simplifies the code below
          dummy-var (gensym "?dummy-var")
          var-join-order (conj var-join-order dummy-var)
          first-var (first var-join-order)
          var->indices (-> (var->indices-map where)
                           (update first-var #(sort-indices % (map idx->iterators %)))
                           #_(initial-indices-sorting triple-idx+var->level idx->trie-iterators)
                           (assoc dummy-var []))
          var->positions (zipmap var-join-order (repeat max-level 0))]
      (loop [res nil partial-row [] var-level 0
             var->indices var->indices var->positions var->positions idx->iterators idx->iterators]
        ;; dummy-level
        (if (= var-level max-level)
          (let [new-var-level (dec var-level)]
            (if-not (neg? new-var-level)
              (let [next-var (nth var-join-order new-var-level)
                    [new-pos idx->iterators] (leapfrog-set-next next-var (get var->positions next-var) var->indices idx->iterators)]
                (recur (cons partial-row res) (pop-empty partial-row) new-var-level
                       var->indices (assoc var->positions next-var new-pos) idx->iterators)
                #_(recur res (pop-empty partial-row) new-var-level
                         var->indices (assoc var->positions next-var new-pos) idx->iterators))

              ;; finished
              (map (partial lookup-row graph) res)))


          ;; move up or down
          (let [_ (assert (< var-level (count var-join-order)))
                var (nth var-join-order var-level)
                [val var-pos idx->iterators] (leapfrog-next var (get var->positions var) var->indices idx->iterators)]
            (if val
              (let [new-var-level (inc var-level)
                    next-var (nth var-join-order new-var-level)
                    idx->iterators (leapfrog-down next-var var->indices idx->iterators)]
                (recur res (conj partial-row val) (inc var-level)
                       (update var->indices next-var #(sort-indices % (map idx->iterators %)))
                       (assoc var->positions var var-pos)
                       idx->iterators))
              (let [new-var-level (dec var-level)]
                (if-not (neg? new-var-level)
                  (let [next-var (nth var-join-order new-var-level)
                        idx->iterators (leapfrog-up var var->indices idx->iterators)
                        [new-pos idx->iterators]
                        (leapfrog-set-next next-var (get var->positions next-var) var->indices idx->iterators)]
                    (recur res (pop-empty partial-row) new-var-level
                           var->indices (assoc var->positions next-var new-pos) idx->iterators))

                  ;; finished
                  (map (partial lookup-row graph) res))))))))
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

  (def q '{:find [?first ?last]
           :where [[?first 5 ?last]]})

  (def q-conformed (s/conform ::h-spec/query q))
  (def compiled-q (-> (query/compile-query2 q (get-db))
                      (assoc :query q-conformed)))


  (join compiled-q (get-db))


  (require 'sc.api)
  (def tupels (sc.api/letsc [5 -3]
                            tuples))

  (defn get-iterator [tuple]
    (graph/get-iterator (db/graph (get-db)) tuple))

  (get-iterator (first tupels) )
  (get-iterator (second tupels) )
  (get-iterator (nth tupels 2) )
  (get-iterator (nth tupels 3) )






  )
