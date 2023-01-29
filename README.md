# Hooray 🎉

This is *very beta*.

Experiments with Datalog and join algorithms.

### Setup
Before running anything you will need to run.
```bash
$ clj -X:deps prep
```

### Examples

```clj
(require '[hooray.core :as h])

(def conn (h/connect "hooray:mem:avl//data"))

(h/transact conn [{:db/id 0
                   :hello :world}
                  [:db/add 1 :foo :bar]])

(h/q '{:find [?e ?a ?v]
       :where [[?e ?a ?v]]}
     (h/db conn))
;; => ([1 :foo :bar] [0 :hello :world])
```

In case you want to experiment with a bigger dataset, you can bring
in the [chinook-db](https://github.com/FiV0/xtdb-chinook).

```clj
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
```

The goal of the library initially was to test different join algorithms.
As an example let us compare the runtime of the triangle query (enumerating
the triangles in a graph) for two different connection types. The
connection type determines the backing storage and the algorithm that will
be used.
```clj
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
```

### Known issues
- For certain connection types having the same variable multiple times in a clause (`[?a ?a ?a]`)
currently does not work. We are aware of that and just have not decided how to deal
with the unification in that case.

### Join Algorithms

Currently the repository implements 3 join algorithms:
- Hash Join
- Leapfrog Triejoin ( https://arxiv.org/pdf/1210.0481.pdf )
- Generic Join (a variation of Generic-Join https://arxiv.org/abs/1310.3314 )

Some others that might be interesting to add for comparison.
- Yannakakis algorithm ( the standard algorithm for non-cyclic queries)
- Something that combines standard binary joins with WCOJ (see Free Join)

### Persistency

There are two remote persistent key/value store options for Redis and FoundationDB. The queries are
still very slow as there is no key/value cache for a query. This part is even more experimental
than the repository itself. These stores currently do not store any history. The idea is
to get an understanding if it's possible to build a DB on a remote key/value store to get separation
of storage from compute without the need to build a custom storage solution.

```clj
(ns persistency
  (:require [hooray.core :as h]
            [hooray.graph-gen :as g-gen]))

(def config-map-redis {:type :per
                       :sub-type :redis
                       :name "hello"
                       :algo :leapfrog
                       :spec {:uri "redis://localhost:6379/"}})

(def redis-conn (h/connect config-map-redis))

(def random-graph (g-gen/random-graph 300 0.3))
(h/transact redis-conn (g-gen/graph->ops random-graph))

(def triangle-query '{:find [?a ?b ?c]
                      :where [[?a :g/to ?b]
                              [?a :g/to ?c]
                              [?b :g/to ?c]]})

(time (count (h/q triangle-query (h/db redis-conn))))
;; "Elapsed time: 22029.502057 msecs"
```

### Inspiration
Some projects I have looked at and drawn some inspiration from

- [asami](https://github.com/quoll/asami) - Graph database written Clojure(script)
- [xtdb](https://github.com/xtdb/xtdb) - General-purpose bitemporal database for SQL, Datalog & graph queries.

### Outlook
Some possible directions and improvements of this repository.

- implement some more join algorithms
- use B-trees
- create a test suite to compare different join algorithms
- persistency with history
- views (Differential Dataflow, Materialized views (see [relic](https://github.com/wotbrew/relic)))

## License

I copied two files from [asami](https://github.com/quoll/asami) in the beginning which are licenced
under the EPL 1.0. The files are marked explicitly in the header.

Everything else is under the MIT Licence. In some cases copyrighted to a different party as I copied some code.
See the `LICENCE` file and the headers of files.
