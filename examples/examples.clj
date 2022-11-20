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

;; triangle query examples

(require '[hooray.graph-gen :as graph-gen])

;; generate a random graph with 300 nodes and edge probability 0.3 as an edge list
(def random-graph (graph-gen/random-graph 300 0.3))

;; using standard clojure maps and hash join
(def conn-hash-join (h/connect "hooray:mem://data"))
;; using avl maps with a generic WCOJ
(def conn-avl-generic-join  (h/connect "hooray:mem:avl:generic//data"))

(h/transact conn-hash-join (graph-gen/graph->ops random-graph))
(h/transact conn-avl-generic-join (graph-gen/graph->ops random-graph))

(def triangle-query '{:find [?a ?b ?c]
                      :where [[?a :g/to ?b]
                              [?a :g/to ?c]
                              [?b :g/to ?c]]})

(time (count (h/q triangle-query (h/db conn-hash-join))))
;; "Elapsed time: 2835.54778 msecs"
(time (count (h/q triangle-query (h/db conn-avl-generic-join))))
;; "Elapsed time: 717.257827 msecs"
