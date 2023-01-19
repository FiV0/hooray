(ns hooray.algo.leapfrog
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [hooray.db :as db]
            [hooray.db.iterator :as itr]
            [hooray.graph :as graph]
            [hooray.query.spec :as h-spec]
            [hooray.util :as util]
            [hooray.util.lru :as lru]))

;; Leapfrog Triejoin
;; https://arxiv.org/pdf/1210.0481.pdf

(comment
  (def q '{:find [?name ?album]
           :where [[?t :track/name "For Those About To Rock (We Salute You)" ]
                   [?t :track/album ?album]
                   [?album :album/artist ?artist]
                   [?artist :artist/name ?name]]
           ;; :keys [name album]
           })

  (def q-conformed (s/conform ::h-spec/query q)))

(defn- iterator-key [iterators p]
  (itr/key (nth iterators p)))

(defn- iterator-next [iterators p]
  {:pre [(vector? iterators)]}
  (update iterators p itr/next))

(defn- iterator-seek [iterators p x]
  {:pre [(vector? iterators)]}
  (update iterators p #(itr/seek % x)))

(defn- iterator-end? [iterators p]
  (itr/at-end? (nth iterators p)))

(defn- lookup-row [graph row]
  (into [] (graph/hashs->values graph row)))

(defn- next-var-index [pattern var]
  (cond
    (= (nth pattern 0) var) 0
    (= (nth pattern 1) var) 1
    (= (nth pattern 2) var) 2
    :else (throw (ex-info "Var not in pattern!" {:var var :pattern pattern}))))

(def idx->name {0 :e 1 :a 2 :v})

(defn- pos->literal [var? var->bindings partial-row cache]
  (cond (util/constant? var?)
        (lru/get cache var?)
        (< (var->bindings var?) (clojure.core/count partial-row))
        (nth partial-row (var->bindings var?))
        :else nil))

(defn partial->tuple-fn [pattern var->bindings cache]
  (fn partial-row->tuple [partial-row next-var]
    (let [next-var-idx (next-var-index pattern next-var)
          [i j] (vec (set/difference #{0 1 2} #{next-var-idx}))
          i-literal (pos->literal (nth pattern i) var->bindings partial-row cache)
          j-literal (pos->literal (nth pattern j) var->bindings partial-row cache)]
      (cond-> {:triple-order [] :triple []}

        i-literal
        (-> (update :triple-order conj (idx->name i))
            (update :triple conj i-literal))

        j-literal
        (-> (update :triple-order conj (idx->name j))
            (update :triple conj j-literal))

        :always
        (-> (update :triple-order conj (idx->name next-var-idx))
            (update :triple conj next-var))))))

(defn vars->tuple-fns [where var->bindings cache]
  (->>
   (map #(vector (filter util/variable? %) (partial->tuple-fn % var->bindings cache)) where)
   (reduce (fn [mapping [clause tuple-fn]]
             (reduce #(update %1 %2 (fnil conj []) tuple-fn) mapping clause)) {})))

(s/def ::vars->tuple-fns (s/map-of util/variable? (s/and vector? (s/* fn?))))

(comment
  (let [var-join-order '[?genre ?t ?name]
        var->bindings (zipmap var-join-order (range))]
    (vars->tuple-fns
     '[[?genre :genre/name "Rock"]
       [?t :track/genre ?genre]
       [?t :track/name ?name]]
     var-join-order
     var->bindings)))


(s/def :leap/iterators (s/and vector? (s/* :leap/iterator)) )
(s/def :leap/position int?)

(defn- iterators-sorted? [{:keys [iterators position]}]
  (let [heads (->> (concat (drop position iterators) (take position iterators))
                   (map itr/key))]
    (= heads (sort heads))))

(s/def ::iterators (s/and (s/keys :req-un [:leap/iterators :leap/position])
                          iterators-sorted?))

(defrecord Iterators [iterators position])

(defn ->iterators
  ([iterators] (->iterators compare iterators))
  ([compare-fn iterators] (->Iterators (vec (sort-by #(itr/key %) compare-fn iterators)) 0)))

(s/fdef var->iterators :args (s/cat :var util/variable?
                                    :partial-row (s/and vector? (s/* int?))
                                    :vars->tuple-fns ::vars->tuple-fns
                                    :graph #(satisfies? graph/GraphIndex %)))

(defn var->iterators [var partial-row vars->tuple-fns graph compare-fn]
  (let [graph-type (-> graph :opts :type)]
    (->> (vars->tuple-fns var)
         (map (fn [partial-row->tuple-fn]
                (graph/get-iterator graph (partial-row->tuple-fn partial-row var) graph-type)))
         (->iterators compare-fn))))

(s/fdef leapfrog-next :args (s/cat :iterators ::iterators))

(defn leapfrog-next [{:keys [iterators position]}]
  (let [k (count iterators)]
    (loop [p position itrs iterators]
      (let [x' (iterator-key itrs (mod (dec p) k))
            x (iterator-key itrs p)]
        (cond
          (or (nil? x) (nil? x'))
          [nil (->Iterators itrs p)]

          (= x' x)
          [x (->Iterators (iterator-next itrs p) (mod (inc p) k))]

          :else
          (let [itrs (iterator-seek itrs p x')]
            (if (iterator-end? itrs p)
              [nil (->Iterators itrs p)]
              (recur (mod (inc p) k) itrs))))))))

(defn join [{:keys [query var-join-order var->bindings] :as _compiled-q} db]
  {:pre [(vector? var-join-order)]}
  (if-let [where (:where query)]
    (let [max-level (count var-join-order)
          ;; dummy var to bottom out
          var-join-order (conj var-join-order (gensym "?dummy"))
          graph (db/graph db)
          compare-fn (db/get-comp db)
          cache (lru/create-lru-cache 128 (db/get-hash-fn db))
          vars->tuple-fns (vars->tuple-fns where var->bindings cache)
          iterators (var->iterators (first var-join-order) [] vars->tuple-fns graph compare-fn)]
      (loop [res nil partial-row [] var-level 0 iterators iterators iterator-stack []]
        (if (= var-level max-level) ;; bottomed out
          (do
            ;; (println "bottom" (map pack/bb->hash partial-row))
            (recur (cons partial-row res) (pop partial-row) (dec var-level) (peek iterator-stack) (pop iterator-stack)))

          (let [[val new-iterators] (leapfrog-next iterators)]
            (cond (not (nil? val))
                  (let [partial-row (conj partial-row val)
                        var-level (inc var-level)]
                    ;; (println "not nil" (map pack/bb->hash partial-row))
                    (recur res partial-row var-level
                           (var->iterators (nth var-join-order var-level) partial-row vars->tuple-fns graph compare-fn)
                           (conj iterator-stack new-iterators)))

                  ;; finished
                  (= var-level 0)
                  (map (partial lookup-row graph) res)

                  ;; moving up
                  :else
                  (do
                    ;; (println "moving up" (map pack/bb->hash partial-row))
                    (recur res (pop partial-row) (dec var-level) (peek iterator-stack) (pop iterator-stack))))))))
    (throw (ex-info "Query must contain where clause!" {:query query}))))

(comment
  (require '[clojure.spec.test.alpha :as st])
  (st/instrument)

  (require '[clojure.edn :as edn]
           '[hooray.query :as query])

  (do
    (def data (doall (edn/read-string (slurp "resources/transactions.edn"))))
    (def conn (db/connect "hooray:mem:avl//data"))
    (db/transact conn data)
    (db/transact conn [[:db/add 1 2 3] [:db/add 4 5 6]])
    (defn get-db [] (db/db conn)))

  (def q '{:find [?name ?album]
           :where [[?t :track/name "For Those About To Rock (We Salute You)" ]
                   [?t :track/album ?album]
                   [?album :album/artist ?artist]
                   [?artist :artist/name ?name]]
           ;; :keys [name album]
           })

  (def q '{:find [?name]
           :where [[?foo :genre/name ?name]]})

  (def q '{:find [?name]
           :where [[?genre :genre/name "Rock"]
                   [?track :track/genre ?genre]
                   #_[?track :track/name ?name]]})

  (def q '{:find [?name]
           :where
           [[?genre :genre/name "Rock"]
            [?t :track/genre ?genre]
            [?t :track/name ?name]]})

  (do
    (def compiled-q  (query/compile-query q (get-db)))
    (join compiled-q (get-db))))
