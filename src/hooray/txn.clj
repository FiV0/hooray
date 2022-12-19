(ns hooray.txn
  (:require [clojure.spec.alpha :as s]))

(s/def :hooray/map-transaction map?)
(s/def :hooray/add-transaction #(and (= :db/add (first %)) (vector? %) (= 4 (count %))))
(s/def :hooray/retract-transaction #(and (= :db/retract (first %)) (vector? %) (= 4 (count %))))
(s/def :hooray/transaction (s/or :map :hooray/map-transaction
                                 :add :hooray/add-transaction
                                 :retract :hooray/retract-transaction))
(s/def :hooray/tx-data (s/* :hooray/transaction))

(comment
  (s/valid? :hooray/tx-data [{:db/id "foo"
                              :foo/bar "x"}
                             [:db/add "foo" :is/cool true]]))
