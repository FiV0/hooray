(ns hooray.db.iterator.memory
  "An iterator for intermediate results."
  (:require [hooray.db.memory.graph-index :as g-index]
            [clojure.data.avl :as avl]
            [me.tonsky.persistent-sorted-set :as tonsky-set]))

;;///////////////////////////////////////////////////////////////////////////////
;;===============================================================================
;;                                  Hash index
;;===============================================================================
;;\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

(defprotocol IHashIterator
  (probe [this x])
  (iter [this]))

(defrecord HashIterator [var-name probe-set]
  IHashIterator
  (probe [this x] (probe-set x))
  (iter [this] (seq probe-set)))

(defn ->hash-iterator [var-name data]
  (->HashIterator var-name (set data)))

;;///////////////////////////////////////////////////////////////////////////////
;;===============================================================================
;;                                  Seek index
;;===============================================================================
;;\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

(defmulti ->leap-iterator (fn [dispatch _var-name _data] dispatch))

(defmethod ->leap-iterator :core [_ _var-name data]
  (g-index/->leap-iterator-core (sorted-set data) 1))

(defmethod ->leap-iterator :avl [_ _var-name data]
  (g-index/->leap-iterator-avl (avl/sorted-set data) 1))

(defmethod ->leap-iterator :tonsky [_ _var-name data]
  (g-index/->leap-iterator-tonsky (tonsky-set/sorted-set data) 1))
