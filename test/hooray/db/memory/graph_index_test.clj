(ns hooray.db.memory.graph-index-test
  (:require [clojure.data.avl :as avl]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest testing is] :as t]
            [hooray.db.memory.graph-index :as gi]
            [hooray.db.memory.graph-index :as mem-gi]
            [hooray.fixtures :as fix]
            [hooray.graph :as graph]
            [hooray.query.spec :as h-spec]
            [hooray.util :as util :refer [dissoc-in]])
  (:import (hooray.db.memory.graph_index SimpleIterator)))

(t/use-fixtures :once fix/with-chinook-index-graph)

(def tuple-3-vars (s/conform ::mem-gi/tuple {:triple '[?e ?a ?v]
                                             :triple-order '[:e :a :v]}))

(def tuple-2-vars (s/conform ::mem-gi/tuple {:triple '[?a ?v]
                                             :triple-order '[:a :v]}))

(def tuple-1-vars (s/conform ::mem-gi/tuple {:triple '[?a]
                                             :triple-order '[:a]}))


(deftest iteration-behaviour
  (testing "correctness of iterator implementations"
    (let [it1 (graph/get-iterator fix/*graph* tuple-3-vars)]
      (is (instance? SimpleIterator it1)))))

(comment
  (t/run-test-var #'iteration-behaviour))
