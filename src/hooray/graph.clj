(ns ^{:doc "The graph index API."
      :author "Paula Gearon"}
    hooray.graph)

(defprotocol Graph
  (new-graph [this] "Creates an empty graph of the same type")
  (graph-add [this triple] "Adds triples to the graph")
  (graph-delete [this triple] "Removes triples from the graph")
  (graph-transact [this tx-id assertions retractions]
    "Bulk operation to add and remove multiple statements in a single operation")
  (resolve-triple [this triple]
    "Resolves patterns from the graph, and returns unbound columns only"))
