(ns hooray.db.memory.graph-index-test
  (:refer-clojure :exclude [rand-int])
  (:require [clojure.data.avl :as avl]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest testing is] :as t]
            [hooray.db.memory.graph-index :as mem-gi]
            [hooray.fixtures :as fix]
            [hooray.graph :as g]
            [hooray.query.spec :as h-spec]
            [hooray.util :as util :refer [dissoc-in]])
  (:import (hooray.db.memory.graph_index SimpleIterator LeapIteratorCore LeapIteratorAVL)))

(t/use-fixtures :once fix/with-identity-hash-index-graph)

(comment
  (t/use-fixtures :once fix/with-chinook-index-graph)
  (reset-meta! *ns* {}))

(def tuple-3-vars #_{:triple '[?e ?a ?v]
                     :triple-order '[:e :a :v]}
  (s/conform ::mem-gi/tuple {:triple '[?e ?a ?v]
                             :triple-order '[:e :a :v]}))

(def tuple-2-vars #_{:triple '[?a ?v]
                     :triple-order '[:a :v]}
  (s/conform ::mem-gi/tuple {:triple '[?a ?v]
                             :triple-order '[:a :v]}))

(def tuple-1-vars #_{:triple '[?a]
                     :triple-order '[:a]}
  (s/conform ::mem-gi/tuple {:triple '[?a]
                             :triple-order '[:a]}))

(comment
  (alter-var-root #'mem-gi/hash (constantly identity))
  (def tx-data (->> (range 10)
                    (partition 3)
                    (map #(into [:db/add] %))))

  (def g (-> (mem-gi/memory-graph {:type :core})
             (g/transact tx-data)))

  (def it (g/get-iterator g tuple-3-vars :simple))

  (loop [res {} it it]
    (let [level (mem-gi/level it)]
      (if (= -1 level)
        res
        (recur (update res level (fnil conj []) (mem-gi/key it))
               (move-to-next it)))))

  )

(defn- test-data [n]
  (->> (range n)
       (partition 3)))

(defn- test-graph
  ([data] (test-graph data :core))
  ([data g-type]
   (let [tx-data (map #(into [:db/add] %) data)]
     (-> (mem-gi/memory-graph {:type g-type})
         (g/transact tx-data)))))

(defn- move-up [it]
  (if (and (mem-gi/at-end? it) (pos? (mem-gi/level it)))
    (move-up (mem-gi/next (mem-gi/up it)))
    it))

(defn- move-to-next [it]
  (let [level (mem-gi/level it)]
    (cond (mem-gi/at-end? it)
          (move-up it)

          (< (inc level) (mem-gi/depth it))
          (mem-gi/open it)

          :else
          (let [it (mem-gi/next it)]
            (if (mem-gi/at-end? it)
              (move-to-next it)
              it)))))

(defn third [s] (nth s 2))

(def it-type->ds-type {:simple :core :core :core :avl :avl :tonsky :tonsky})

(deftest simple-iterator-test
  (testing "correctness of simple-iterator"
    (doseq [it-type '(:simple :core :avl :tonsky)]
      (let [tdata [[:db/add 1 2 3]]
            tgraph (-> (mem-gi/memory-graph {:type (it-type->ds-type it-type)})
                       (g/transact tdata))
            it (g/get-iterator tgraph tuple-3-vars it-type)]
        (is (false? (mem-gi/at-end? it)))
        (is (= 1 (mem-gi/key it) ))
        (is (false? (-> it mem-gi/open mem-gi/at-end?)))
        (is (= 2 (-> it mem-gi/open mem-gi/key)))
        (is (false? (-> it mem-gi/open mem-gi/open mem-gi/at-end?)))
        (is (= 3 (-> it mem-gi/open mem-gi/open mem-gi/key)))
        (is (true? (-> it mem-gi/open mem-gi/open mem-gi/next mem-gi/at-end?)))
        (is (true? (-> it mem-gi/open mem-gi/open mem-gi/next mem-gi/up mem-gi/next mem-gi/at-end?)))
        (is (true? (-> it mem-gi/open mem-gi/open mem-gi/next mem-gi/up mem-gi/next
                       mem-gi/up mem-gi/next mem-gi/at-end?)))
        (is (false? (-> it (mem-gi/seek 1) mem-gi/at-end?)))
        (is (true? (-> it (mem-gi/seek 2) mem-gi/at-end?)))
        (is (false? (-> it mem-gi/open (mem-gi/seek 2) mem-gi/at-end?)))
        (is (true? (-> it mem-gi/open (mem-gi/seek 3) mem-gi/at-end?)))
        (is (false? (-> it mem-gi/open mem-gi/open (mem-gi/seek 3) mem-gi/at-end?)))
        (is (true? (-> it mem-gi/open mem-gi/open (mem-gi/seek 4) mem-gi/at-end?)))))))

(comment
  (require '[clojure.data.avl :as avl])
  (let [tdata [[:db/add 1 2 3]]
        tgraph (-> (mem-gi/memory-graph {:type :tonsky})
                   (g/transact tdata))
        it (g/get-iterator tgraph tuple-3-vars :tonsky)]
    (-> it mem-gi/open :index type #_(mem-gi/seek 2))
    ;; (-> it :index (avl/seek 1))
    )
  )

(defn- finished? [it]
  (and (= (mem-gi/level it) 0) (mem-gi/at-end? it)))

(deftest iteration-test-iterator
  (testing "correctness of iterator implementations"
    (doseq [it-type '(:simple :core :avl :tonsky)]
      (let [tdata (test-data 1000)
            tgraph (test-graph tdata (it-type->ds-type it-type))
            it (g/get-iterator tgraph tuple-3-vars it-type)
            it-index->data (loop [res {} it it]
                             (if (finished? it)
                               res
                               (recur (update res (mem-gi/level it) (fnil conj []) (mem-gi/key it))
                                      (move-to-next it))))]
        (is (= (get it-index->data 0) (map first tdata)))
        (is (= (get it-index->data 1) (map second tdata)))
        (is (= (get it-index->data 2) (map third tdata)))))))

(defn rand-int
  ([n] (clojure.core/rand-int n))
  ([a b] (+ (rand-int (- b a)) a)))

(defn- range-k [n k]
  (mapcat #(repeat k %) (range n)))

(defn- more-homogenous-test-data [n k]
  (map vector (range-k n k) (range-k n k) (range n)))

(deftest seek-test-iterator
  (testing "seek correctness of iterator implementations"
    (doseq [it-type '(:simple :core :avl :tonsky)]
      (let [size 1000
            tdata (more-homogenous-test-data 1000 7)
            tgraph (test-graph tdata (it-type->ds-type it-type))
            it (g/get-iterator tgraph tuple-3-vars it-type)
            it-index->data (loop [res {} it it]
                             (if (finished? it)
                               res
                               (let [_ (assert (mem-gi/key it))
                                     seek-target (rand-int (mem-gi/key it) (min (+ (mem-gi/key it) 3) size))
                                     it (mem-gi/seek it seek-target)
                                     k (mem-gi/key it)]
                                 (if k
                                   (recur (update res (mem-gi/level it) (fnil conj []) k)
                                          (move-to-next it))
                                   (recur res
                                          (move-to-next it))))))]
        (is (= (get it-index->data 0) (sort (get it-index->data 0))))
        (is (= (get it-index->data 1) (sort (get it-index->data 1))))
        (is (= (get it-index->data 2) (sort (get it-index->data 2))))))))

(def tuple-with-literals
  (s/conform ::mem-gi/tuple {:triple '[:foo :bar ?e]
                             :triple-order '[:e :a :v]}))

(deftest get-iterator-literal-test
  (testing "correctness of iterators with literal in tuple"
    (doseq [it-type '(:simple :core :avl :tonsky)]
      (let [tdata [[:foo :bar 1] [:foo :bar 2] [:foo :bar 3]]
            tgraph (test-graph tdata (it-type->ds-type it-type))
            it (g/get-iterator tgraph tuple-with-literals it-type)
            it-index->data (loop [res {} it it]
                             (if (mem-gi/at-end? it)
                               res
                               (recur (update res (mem-gi/level it) (fnil conj []) (mem-gi/key it))
                                      (move-to-next it))))]
        (is (= #{1 2 3} (into #{} (get it-index->data 0))))))))
