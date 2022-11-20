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
As an example let us compare the runtime of the triangle query (counting
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

### Inspiration
Some projects I have looked at and drawn some inspiration from

- [asami](https://github.com/quoll/asami) - Graph database written Clojure(script)
- [xtdb](https://github.com/xtdb/xtdb) - General-purpose bitemporal database for SQL, Datalog & graph queries.

### Outlook
Some possible directions and improvements of this repository.

- implement some more join algorithms
- use B-trees
- create a test suite to compare different join algorithms
- persistency
- views (Differential Dataflow, Materialized views (see [relic](https://github.com/wotbrew/relic)))

## License

I copied two files from [asami](https://github.com/quoll/asami) in the beginning which are licenced
under the EPL 1.0. The files are marked explicitly in the header.

Everything else is under the MIT Licence. In some cases copyrighted to a different party as I copied some code.
See the `LICENCE` file and the headers of files.
