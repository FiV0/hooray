(ns hooray.triangle-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest testing is] :as t]
            [hooray.fixtures :as fix :refer [*conn*]]
            [hooray.core :as h]
            [hooray.graph-gen :as g]))

(def triangle-query '{:find [?a ?b ?c]
                      :where [[?a :g/to ?b]
                              [?a :g/to ?c]
                              [?b :g/to ?c]]})

(t/use-fixtures :each fix/with-each-db-option* fix/with-timing-logged)


(deftest complete-graph-test
  (testing "triangle query complete graph 100"
    (h/transact *conn* (g/graph->ops (g/complete-graph 100)))
    (is (= 161700
           (count (h/q triangle-query (h/db *conn*)))))))


(deftest complete-bipartite-test
  (testing "triangle query bipartite 100"
    (h/transact *conn* (g/graph->ops (g/complete-bipartite 100)))
    (is (= 0
           (count (h/q triangle-query (h/db *conn*)))))))

(deftest star-graph-test
  (testing "triangle query star graph 1000"
    (h/transact *conn* (g/graph->ops (g/star-graph 1000)))
    (is (= 0
           (count (h/q triangle-query (h/db *conn*)))))))

(deftest star-with-ring-graph-test
  (testing "triangle query star graph with ring 1000"
    (h/transact *conn* (g/graph->ops (g/star-with-ring 1000)))
    (is (= 998
           (count (h/q triangle-query (h/db *conn*)))))))

(deftest complete-independents-test
  (testing "triangle query with complete independents 100 10"
    (h/transact *conn* (g/graph->ops (g/complete-independents 100 10)))
    (is (= 120000
           (count (h/q triangle-query (h/db *conn*)))))))

(comment
  (def random-graph (g/random-graph 300 0.3))
  (def random-independents (g/random-independents 300 10 0.3))
  (spit "resources/random-graph-100-0.3.edn" (apply list random-graph))
  (spit "resources/random-independents-graph-100-0.3.edn" (apply list random-independents)))

(def random-graph (clojure.edn/read-string (slurp "resources/random-graph-100-0.3.edn")))
(def random-independents (clojure.edn/read-string (slurp "resources/random-independents-graph-100-0.3.edn")))

(deftest random-graph-test
  (testing "triangle query with random graph 300 0.3"
    (h/transact *conn* (g/graph->ops random-graph))
    (is (= 118968
           (count (h/q triangle-query (h/db *conn*)))))))

(deftest random-independents-test
  (testing "triangle query with random independent graph 300 10 0.3"
    (h/transact *conn* (g/graph->ops random-independents))
    (is (= 85411
           (count (h/q triangle-query (h/db *conn*)))))))
