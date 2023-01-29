(ns hooray.db.iterator
  (:refer-clojure :exclude [key next])
  (:require [clojure.spec.alpha :as s]))

(defprotocol LeapIterator
  (key [this])
  (next [this])
  (seek [this k])
  (at-end? [this]))

(s/def :leap/iterator #(satisfies? LeapIterator %))

(defprotocol LeapLevels
  (open [this])
  (up [this])
  (level [this])
  (depth [this]))

(s/def :leap/levels #(satisfies? LeapLevels %))

(defprotocol IteratorCount
  (count* [this]))

(s/def :itr/count #(satisfies? IteratorCount %))
