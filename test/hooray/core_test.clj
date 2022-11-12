(ns hooray.core-test
  (:require
   [clojure.test :refer [deftest testing is] :as t]
   [hooray.fixtures :as fix :refer [*conn*]]
   [hooray.core :as core]
   [hooray.db :as db]))

(t/use-fixtures :once fix/with-each-db-option* fix/with-chinook-data)
(t/use-fixtures :each fix/with-timing-logged)

(deftest simple-query-test
  (testing "Simple query"
    (is (= [["AC/DC"]]
           (core/q '{:find [?name]
                     :where [[?t :track/name "For Those About To Rock (We Salute You)" ]
                             [?t :track/album ?album]
                             [?album :album/artist ?artist]
                             [?artist :artist/name ?name]]}
                   (db/db *conn*))))))

(deftest simple-multi-result-query
  (testing "Simple multi-result query"
    (is (= 32
           (count (core/q '{:find [?track-name ?album-title]
                            :where [[?artist :artist/name "Ozzy Osbourne"]
                                    [?album :album/artist ?artist]
                                    [?album :album/title ?album-title]
                                    [?t :track/album ?album]
                                    [?t :track/name ?track-name]]}
                          (db/db *conn*)))))))
