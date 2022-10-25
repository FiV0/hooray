(ns examples)

(require '[hooray.core :as h])

(def conn (h/connect "hooray:mem:avl//data"))

(h/transact conn [{:db/id 0
                   :hello :world}
                  [:db/add 1 :foo :bar]])

(h/q '{:find [?e ?a ?v]
       :where [[?e ?a ?v]]}
     (h/db conn))
;; => ([1 :foo :bar] [0 :hello :world])

;; chinook-db example

(require '[clojure.edn :as edn])

(def data (edn/read-string (slurp "resources/transactions.edn")))
(h/transact conn data)

(h/q '{:find [?name ?album-title]
       :where [[?t :track/name "For Those About To Rock (We Salute You)" ]
               [?t :track/album ?album]
               [?album :album/title ?album-title]
               [?album :album/artist ?artist]
               [?artist :artist/name ?name]]}
     (h/db conn))
;; => (["AC/DC" "For Those About To Rock We Salute You"])
