(ns hooray.triangle-test
  (:require
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
  (testing "triangle query 100"
    (h/transact *conn* (g/graph->ops (g/complete-graph 100)))
    (is (= nil
           (count (h/q triangle-query (h/db *conn*)))))))
