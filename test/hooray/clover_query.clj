(ns hooray.clover-query
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest testing is] :as t]
            [hooray.fixtures :as fix :refer [*conn*]]
            [hooray.core :as h]
            [hooray.graph-gen :as g]))

(def clover-query '{:find [?x ?a ?b ?c]
                    :where [[?x :r ?a]
                            [?x :s ?b]
                            [?x :t ?c]]})

;; instance from
;; Free Join: Unifying Worst-Cast Optimal and Traditional Joins
;; http://arxiv.org/abs/2301.10841

(defn- clover-instance [n]
  (concat [[0 :r 0]
           [0 :s 0]
           [0 :t 0]]
          (apply concat (for [i (range n)]
                          [[1 :r (str "a_l_" i)]
                           [2 :r (str "a_r_" i)]
                           [2 :s (str "b_l_" i)]
                           [3 :s (str "b_r_" i)]
                           [3 :t (str "c_l_" i)]
                           [1 :t (str "c_r_" i)]]))))

(comment
  (clover-instance 10))

(t/use-fixtures :each fix/with-println fix/with-each-db-option*)

(deftest clover-query-test
  (testing "clover query test"
    (h/transact *conn* (map #(into [:db/add] %) (clover-instance 2000)))
    (fix/with-timing-logged*
      (is (= 1
             (count (h/q clover-query (h/db *conn*))))))))
