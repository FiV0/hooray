(ns hooray.util.persistent-map
  (:require [me.tonsky.persistent-sorted-set :as set])
  (:import
   (clojure.lang RT Util APersistentMap APersistentSet
                 IPersistentMap IPersistentSet IPersistentStack
                 Box MapEntry SeqIterator)
   (me.tonsky.persistent_sorted_set PersistentSortedSet)
   (java.util Comparator )))

(defprotocol getSet
  (get-set [this]))

(declare create-seq)

(deftype PersistentSortedMap [^PersistentSortedSet set ^IPersistentMap _meta]
  Object
  (toString [this]
    (RT/printString this))

  clojure.lang.IMeta
  (meta [this]
    _meta)

  clojure.lang.IObj
  (withMeta [this meta]
    (PersistentSortedMap set))

  clojure.lang.Counted
  (count [this]
    (count set))

  getSet
  (get-set [this] set)

  clojure.lang.IPersistentCollection
  (cons [this entry]
    (if (vector? entry)
      (assoc this (nth entry 0) (nth entry 1))
      (reduce conj this entry)))

  (empty [this]
    (PersistentSortedSet. (empty set) {}))

  (equiv [this that]
    (if (instance? PersistentSortedMap that)
      (= set (get-set that))
      false))

  clojure.lang.IFn
  (invoke [this k]
    (.valAt this k))

  (invoke [this k not-found]
    (.valAt this k not-found))

  (applyTo [this args]
    (let [n (RT/boundedLength args 2)]
      (case n
        0 (throw (clojure.lang.ArityException.
                  n (.. this (getClass) (getSimpleName))))
        1 (.invoke this (first args))
        2 (.invoke this (first args) (second args))
        3 (throw (clojure.lang.ArityException.
                  n (.. this (getClass) (getSimpleName)))))))

  clojure.lang.Seqable
  (seq [this]
    (when (pos? (count set))
      (create-seq set true)))

  clojure.lang.Reversible
  (rseq [this]
    (when (pos? (count set))
      (create-seq set false)))

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))

  ;; TO FIX
  (valAt [this k not-found]
    (if-let [n (get set [k nil])]
      (second n)
      not-found))

  clojure.lang.Associative
  (assoc [this k v]
    (PersistentSortedMap. (conj set [k v]) _meta))

  (containsKey [this k]
    (not (nil? (.entryAt this k))))

  ;; TO FIX
  (entryAt [this k]
    (if-let [n (get set [k nil])]
      (MapEntry. (first n) (second n))))

  clojure.lang.MapEquivalence
  clojure.lang.IPersistentMap
  (without [this k]
    (PersistentSortedMap. (disj set [k nil]) _meta))

  (assocEx [this k v]
    (throw (UnsupportedOperationException.))))

(defn- create-seq [set ascending?]
  (let [seq-fn (if ascending? seq rseq)]
    (->> set seq-fn (map (fn [[k v]] (MapEntry. k v))))))

(defn persistent-sorted-map
  ([] (PersistentSortedMap. (set/sorted-set) {}))
  ([& kvs] (into (persistent-sorted-map) kvs)))

(comment
  (seq (persistent-sorted-map [1 2] [3 4])))
