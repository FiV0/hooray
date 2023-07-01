;; copied from xtdb 1.x

(ns hooray.query-test
  (:require [clojure.test :as t]
            [hooray.core :as h]
            [hooray.fixtures :as fix :refer [*conn*]]
            [hooray.query :as q]))

(t/use-fixtures :each fix/with-each-db-option*)

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

(t/deftest test-returning-maps
  (h/transact *conn* [{:xt/id :ivan :name "Ivan" :last-name "Ivanov"}
                      {:xt/id :petr :name "Petr" :last-name "Petrov"}])

  (let [db (h/db *conn*)]
    (t/is (= #{{:user/name "Ivan", :user/last-name "Ivanov"}
               {:user/name "Petr", :user/last-name "Petrov"}}
             (-> (h/q '{:find [?name ?last-name]
                        :keys [user/name user/last-name]
                        :where [[e :name ?name]
                                [e :last-name ?last-name]]}
                      db)
                 set)))

    (t/is (= #{{'user/name "Ivan", 'user/last-name "Ivanov"}
               {'user/name "Petr", 'user/last-name "Petrov"}}
             (-> (h/q '{:find [?name ?last-name]
                        :syms [user/name user/last-name]
                        :where [[e :name ?name]
                                [e :last-name ?last-name]]}
                      db)
                 set)))

    (t/is (= #{{"name" "Ivan", "last-name" "Ivanov"}
               {"name" "Petr", "last-name" "Petrov"}}
             (-> (h/q '{:find [?name ?last-name]
                        :strs [name last-name]
                        :where [[e :name ?name]
                                [e :last-name ?last-name]]}
                      db)
                 set)))

    (t/is (thrown? IllegalArgumentException
                   (h/q '{:find [name last-name]
                          :keys [name]
                          :where [[e :name name]
                                  [e :last-name last-name]
                                  [e :name "Ivan"]
                                  [e :last-name "Ivanov"]]}
                        db))
          "throws on arity mismatch")))

(t/deftest test-query-with-in-bindings
  (h/transact *conn* [{:db/id :ivan :name "Ivan" :last-name "Ivanov"}
                      {:db/id :petr :name "Petr" :last-name "Petrov"}])
  (let [db (h/db *conn*)]

    (t/is (= [[:ivan]]
             (h/q '{:find [e]
                    :in [$ name]
                    :where [[e :name name]]}
                  db
                  "Ivan")))

    (t/testing "the db var is optional (only option for now)"
      (t/is (= [[:ivan]]
               (h/q '{:find [e]
                      :in [name]
                      :where [[e :name name]]}
                    db
                    "Ivan"))))

    (t/is (= [[:ivan]]
             (h/q '{:find [e]
                    :in [$ name last-name]
                    :where [[e :name name]
                            [e :last-name last-name]]}
                  db
                  "Ivan" "Ivanov")))


    (t/is (= [[:ivan]]
             (h/q
              '{:find [e]
                :in [$ [name]]
                :where [[e :name name]]}
              db
              ["Ivan"])))

    (t/is (= '([:ivan] [:petr])
             (h/q
              '{:find [e]
                :in [$ [[name]]]
                :where [[e :name name]]}
              db
              [["Ivan"] ["Petr"]])))

    #_(t/is (= nil
               (h/q
                '{:find [e]
                  :in [$ [name ...]]
                  :where [[e :name name]]}
                db
                ["Ivan" "Petr"]))))
  #_#_#_#_
  (t/testing "can access the db"
    (t/is (= #{["class hdb.query.QueryDatasource"]} (h/q (h/db *conn*) '{:find [ts]
                                                                         :in [$]
                                                                         :where [[(str t) ts]
                                                                                 [(type $) t]]}))))
  (t/testing "where clause is optional"
    (t/is (= #{[1]} (h/q (h/db *conn*)
                         '{:find [x]
                           :in [$ x]}
                         1))))

  (t/testing "can use both args and in"
    (t/is (= #{[2]} (h/q (h/db *conn*)
                         '{:find [x]
                           :in [$ [x ...]]
                           :args [{:x 1}
                                  {:x 2}]}
                         [2 3]))))

  (t/testing "var bindings need to be distinct"
    (t/is (thrown-with-msg?
           IllegalArgumentException
           #"In binding variables not distinct"
           (h/q (h/db *conn*) '{:find [x]
                                :in [$ [x x]]} [1 1])))))