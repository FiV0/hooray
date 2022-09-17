(ns hooray.db.memory.graph-index-test
  (:require [clojure.data.avl :as avl]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest testing is] :as t]
            [hooray.db.memory.graph-index :as gi]
            [hooray.db.memory.graph-index :as mem-gi]
            [hooray.fixtures :as fix]
            [hooray.graph :as g]
            [hooray.query.spec :as h-spec]
            [hooray.util :as util :refer [dissoc-in]])
  (:import (hooray.db.memory.graph_index SimpleIterator)))

(comment
  (t/use-fixtures :once fix/with-chinook-index-graph)
  (reset-meta! *ns* {}))

(def tuple-3-vars {:triple '[?e ?a ?v]
                   :triple-order '[:e :a :v]}
  #_(s/conform ::mem-gi/tuple {:triple '[?e ?a ?v]
                               :triple-order '[:e :a :v]}))

(def tuple-2-vars {:triple '[?a ?v]
                   :triple-order '[:a :v]}
  #_(s/conform ::mem-gi/tuple {:triple '[?a ?v]
                               :triple-order '[:a :v]}))

(def tuple-1-vars {:triple '[?a]
                   :triple-order '[:a]}
  #_(s/conform ::mem-gi/tuple {:triple '[?a]
                               :triple-order '[:a]}))

(alter-var-root #'mem-gi/hash (constantly identity))

(comment
  (mem-gi/hash 1)

  (def tx-data (->> (range 10)
                    (partition 3)
                    (map #(into [:db/add] %))))

  (def g (-> (mem-gi/memory-graph {:type :core})
             (g/transact tx-data)))

  (def it (g/get-iterator g tuple-3-vars))

  (loop [res {} it it]
    (let [level (mem-gi/level it)]
      (if (= -1 level)
        res
        (recur (update res level (fnil conj []) (mem-gi/key it))
               (if (= (inc level) (mem-gi/depth it))
                 (move-to-next it)
                 (mem-gi/open it))))))

  )

(defn- test-data [n]
  (->> (range n)
       (partition 3)))

(defn- test-graph [data]
  (let [tx-data (map #(into [:db/add] %) data)]
    (-> (mem-gi/memory-graph {:type :core})
        (g/transact tx-data))))

(def pos-or-zero? (complement neg?))

(defn- move-up [it]
  (if (and (mem-gi/at-end? it) (pos-or-zero? (mem-gi/level it)))
    (move-up (mem-gi/up it))
    it))

(defn- move-to-next [it]
  (let [it (move-up it)]
    (if (neg? (mem-gi/level it))
      it
      (mem-gi/next it))))

(defn third [s] (nth s 2))

(deftest iteration-test-simple-iterator
  (testing "correctness of iterator implementations"
    (let [tdata (test-data 1000)
          tgraph (test-graph tdata)
          it (g/get-iterator tgraph tuple-3-vars)
          it-index->data (loop [res {} it it]
                           (let [level (mem-gi/level it)]
                             (if (= -1 level)
                               res
                               (recur (update res level (fnil conj []) (mem-gi/key it))
                                      (if (= (inc level) (mem-gi/depth it))
                                        (move-to-next it)
                                        (mem-gi/open it))))))]
      (is (instance? SimpleIterator it))
      (is (= (get it-index->data 0) (map first tdata)))
      (is (= (get it-index->data 1) (map second tdata)))
      (is (= (get it-index->data 2) (map third tdata))))))

(comment
  (t/run-test-var #'iteration-test-simple-iterator))
