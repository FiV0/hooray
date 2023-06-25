;; copied from xtdb 1.x

(ns hooray.query-test
  (:require [clojure.test :as t]
            [hooray.core :as h]
            [hooray.fixtures :as fix :refer [*conn*]]
            [hooray.query :as q]))

(t/use-fixtures :once fix/with-each-db-option*)

(t/deftest test-sanity-check
  (h/transact *conn* [{:name "Ivan"}])
  (t/is (first (h/q '{:find [e]
                      :where [[e :name "Ivan"]]}
                    (h/db *conn*)))))


(t/deftest test-basic-query
  (h/transact *conn* [{:db/id :ivan :name "Ivan" :last-name "Ivanov"} {:db/id :petr :name "Petr" :last-name "Petrov"}])

  (t/testing "Can query value by single field"
    (t/is (= [["Ivan"]] (h/q  '{:find [name]
                                :where [[e :name "Ivan"]
                                        [e :name name]]}
                              (h/db *conn*))))
    (t/is (= [["Petr"]] (h/q '{:find [name]
                               :where [[e :name "Petr"]
                                       [e :name name]]}
                             (h/db *conn*)))))

  (t/testing "Can query entity by single field"
    (t/is (= [[:ivan]] (h/q '{:find [e]
                              :where [[e :name "Ivan"]]}
                            (h/db *conn*))))
    (t/is (= [[:petr]] (h/q '{:find [e]
                              :where [[e :name "Petr"]]}
                            (h/db *conn*)))))

  (t/testing "Can query using multiple terms"
    (t/is (= [["Ivan" "Ivanov"]]
             (h/q '{:find [name last-name]
                    :where [[e :name name]
                            [e :last-name last-name]
                            [e :name "Ivan"]
                            [e :last-name "Ivanov"]]}
                  (h/db *conn*)))))

  (t/testing "Negate query based on subsequent non-matching clause"
    (t/is (= [] (h/q '{:find [e]
                       :where [[e :name "Ivan"]
                               [e :last-name "Ivanov-does-not-match"]]}
                     (h/db *conn*)))))

  (t/testing "Can query for multiple results"
    (t/is (= #{["Ivan"] ["Petr"]}
             (-> (h/q '{:find [name] :where [[e :name name]]}
                      (h/db *conn*))
                 set))))

  (h/transact *conn* [{:db/id :smith :name "Smith" :last-name "Smith"}])
  (t/testing "Can query across fields for same value"
    (t/is (= [[:smith]]
             (h/q '{:find [p1] :where [[p1 :name name]
                                       [p1 :last-name name]]}
                  (h/db *conn*)))))

  (t/testing "Can query across fields for same value when value is passed in"
    (t/is (= [[:smith]]
             (h/q '{:find [p1] :where [[p1 :name name]
                                       [p1 :last-name name]
                                       [p1 :name "Smith"]]}
                  (h/db *conn*))))))
