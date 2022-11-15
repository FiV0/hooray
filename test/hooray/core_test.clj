(ns hooray.core-test
  (:require
   [clojure.test :refer [deftest testing is] :as t]
   [hooray.fixtures :as fix :refer [*conn*]]
   [hooray.core :as h]
   [hooray.db :as db]))

(t/use-fixtures :once fix/with-each-db-option*)
(t/use-fixtures :each fix/with-timing-logged)

(deftest empty-db-test
  (testing "empty db"
    (is (empty?
         (h/q '{:find [?a]
                :where [[:bar :bar ?a]]}
              (db/db *conn*))))

    (is (empty?
         (h/q '{:find [?a]
                :where [[:bar ?a :bar]]}
              (db/db *conn*))))

    (is (empty?
         (h/q '{:find [?a]
                :where [[?a :bar :bar]]}
              (db/db *conn*))))))

(deftest simple-query-test
  (testing "Simple query"
    (h/transact *conn* [[:db/add :foo :foo :foo] [:db/add :bar :bar :foo]
                        [:db/add :bar :foo :bar] [:db/add :foo :bar :bar]])
    (is (= [[:foo]]
           (h/q '{:find [?a]
                  :where [[:bar :bar ?a]]}
                (db/db *conn*))))

    (is (= [[:foo]]
           (h/q '{:find [?a]
                  :where [[:bar ?a :bar]]}
                (db/db *conn*))))

    (is (= [[:foo]]
           (h/q '{:find [?a]
                  :where [[?a :bar :bar]]}
                (db/db *conn*))))))


(deftest unification-query-test
  (testing "unification"
    (h/transact *conn* [[:db/add :foo :foo :foo] [:db/add :bar :bar :foo]
                        [:db/add :bar :foo :bar] [:db/add :foo :bar :bar]])
    (is (= [[:foo]]
           (h/q '{:find [?a]
                  :where [[:bar :bar ?a]
                          [:bar ?a :bar]]}
                (db/db *conn*))))

    (is (= [[:foo]]
           (h/q '{:find [?a]
                  :where [[:bar ?a :bar]
                          [?a :bar :bar]]}
                (db/db *conn*))))

    (is (= [[:foo]]
           (h/q '{:find [?a]
                  :where [[?a :bar :bar]
                          [:bar :bar ?a]]}
                (db/db *conn*))))))

;; TODO broken on every db so far
#_(ns-unmap *ns* 'pattern-with-same-var-test)

(deftest pattern-with-same-var-test
  (testing "same var multiple times in pattern"
    (h/transact *conn* [[:db/add :foo :foo :foo]
                        [:db/add :bar :bar :foo]
                        [:db/add :bar :toto :foo]
                        [:db/add :bar :foo :bar]
                        [:db/add :bar :foo :toto]
                        [:db/add :foo :bar :bar]
                        [:db/add :foo :bar :toto]])
    (let [res (h/q '{:find [?a]
                     :where [[?a ?a :foo]]}
                   (db/db *conn*))]
      (is (= 2 (count res)))
      (is (= #{[:foo] [:bar]} (set res))))
    (let [res (h/q '{:find [?a ?b]
                     :where [[?a ?a ?b]]}
                   (db/db *conn*))]
      (is (= 2 (count res)))
      (is (= #{[:foo :foo] [:bar :foo]} (set res))))
    (let [res (h/q '{:find [?a]
                     :where [[?a :foo ?a]]}
                   (db/db *conn*))]
      (is (= 2 (count res)))
      (is (= #{[:foo] [:bar]} (set res))))
    (let [res (h/q '{:find [?a ?b]
                     :where [[?a ?b ?b]]}
                   (db/db *conn*))]
      (is (= 2 (count res)))
      (is (= #{[:foo :foo] [:foo :bar]} (set res))))
    (let [res (h/q '{:find [?a]
                     :where [[:foo ?a ?a]]}
                   (db/db *conn*))]
      (is (= 2 (count res)))
      (is (= #{[:foo] [:bar]} (set res))))
    (let [res (h/q '{:find [?a ?b]
                     :where [[?a ?b ?a]]}
                   (db/db *conn*))]
      (is (= 2 (count res)))
      (is (= #{[:foo :foo] [:bar :foo]} (set res))))
    (is (= [[:foo]]
           (h/q '{:find [?a]
                  :where [[?a ?a ?a]]}
                (db/db *conn*))))))
