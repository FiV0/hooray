(ns hooray.util.persistent-map
  (:refer-clojure :exclude [sorted-map sorted-map-by])
  (:require [me.tonsky.persistent-sorted-set :as set])
  (:import
   (clojure.lang RT IPersistentMap MapEntry SeqIterator)
   (me.tonsky.persistent_sorted_set PersistentSortedSet ISeek Seq)))

#_(defprotocol ISeek
    (seek [this k]))

(deftype PersistentSortedMapSeq [^Seq set-seq]
  clojure.lang.Seqable
  (seq [this]
    this)

  clojure.lang.Sequential
  clojure.lang.ISeq
  (first [this]
    (when-let [e (first set-seq)]
      (MapEntry. (first e) (second e))))

  (more [this]
    (if-let [next-seq (next set-seq)]
      (PersistentSortedMapSeq. next-seq)
      ()))

  (next [this]
    (.seq (.more this)))

  ISeek
  (seek [this k]
    (when-let [new-seq (set/seek set-seq [k nil])]
      (PersistentSortedMapSeq. new-seq))))

(defn seek [seq k]
  (.seek seq k))

(defprotocol getSet
  (get-set [this]))

(deftype PersistentSortedMap [^PersistentSortedSet set ^IPersistentMap _meta]
  Object
  (toString [this]
    (RT/printString this))

  clojure.lang.IMeta
  (meta [this]
    _meta)

  clojure.lang.IObj
  (withMeta [this meta]
    (PersistentSortedMap. set meta))

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
      (PersistentSortedMapSeq. (seq set))))

  clojure.lang.Reversible
  (rseq [this]
    (when (pos? (count set))
      (PersistentSortedMapSeq. (rseq set))))

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
    ;; the `disjoin` is needed as we o/w don't get an update
    (PersistentSortedMap. (-> set (disj [k]) (conj [k v])) _meta))

  (containsKey [this k]
    (not (nil? (.entryAt this k))))

  ;; TO FIX
  (entryAt [this k]
    (when-let [n (get set [k nil])]
      (MapEntry. (first n) (second n))))

  clojure.lang.MapEquivalence
  clojure.lang.IPersistentMap
  (without [this k]
    (PersistentSortedMap. (disj set [k nil]) _meta))

  (assocEx [this k v]
    (throw (UnsupportedOperationException.)))

  Iterable
  (iterator [this] (SeqIterator. (seq this)))
  (forEach [this action] (throw (UnsupportedOperationException.)))
  (spliterator [this] (throw (UnsupportedOperationException.))))

(defn sorted-map
  ([] (PersistentSortedMap. (set/sorted-set-by #(compare (first %1) (first %2))) {}))
  ([& kvs] (into (sorted-map) kvs)))

(comment
  (seq (sorted-map [1 2] [3 4]))
  (-> (seq (sorted-map [1 2] [3 4]))
      (seek 2)))

(defn sorted-map-by
  ([cmp] (PersistentSortedMap. (set/sorted-set-by #(cmp (first %1) (first %2))) {}))
  ([cmp & kvs] (into (sorted-map-by cmp) kvs)))

(comment
  (def key-fn -)
  (def cmp #(compare (key-fn %1) (key-fn %2)))

  (sorted-map-by cmp [1 2] [2 3])
  (-> (sorted-map-by cmp [1 2] [2 3])
      seq
      (seek 1))

  (sorted-map [1 (sorted-map [2 3])])
  (get (sorted-map [1 (sorted-map [2 3])]) 1)
  (into [] (sorted-map [1 2] [2 3])))
