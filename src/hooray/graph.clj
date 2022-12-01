;; Copyright © 2016-2021 Cisco Systems Copyright © 2015-2022 Paula Gearon
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "The graph index API."}
    hooray.graph)

(defprotocol Graph
  (new-graph
    [this]
    [this opts] "Creates an empty graph of the same type")
  (graph-add [this triple] "Adds triples to the graph")
  (graph-delete [this triple] "Removes triples from the graph")
  (graph-transact [this tx-id assertions retractions]
    "Bulk operation to add and remove multiple statements in a single operation")
  (resolve-triple
    [this triple]
    [this triple ts]
    "Resolves patterns from the graph, and returns unbound columns only")
  (transact
    [this tx-data]
    [this tx-data ts]
    "Bulk operation to add and remove multiple statements in a single operation"))

(defprotocol GraphIndex
  (resolve-tuple [this tuple])
  (get-iterator
    [this tuple]
    [this tuple type]))

(defprotocol BitempGraph
  (in-between [this t1 t2] "Creates a graph with only facts "))
