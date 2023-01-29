(ns hooray.db.persistent.protocols-test
  (:refer-clojure :exclude [rand-int])
  (:require [clojure.data.avl :as avl]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest testing is] :as t]
            [hooray.db.memory.graph-index :as mem-gi]
            [hooray.fixtures :as fix]
            [hooray.graph :as g]
            [hooray.db.iterator :as itr]
            [hooray.query.spec :as h-spec]
            [hooray.util :as util :refer [dissoc-in]]
            [hooray.db.persistent.graph :as per-g]
            [hooray.db.persistent.protocols :as proto]
            [hooray.db.persistent.packing :as pack]
            [hooray.db.persistent.redis :as redis])
  (:import (hooray.db.memory.graph_index SimpleIterator LeapIteratorCore LeapIteratorAVL)))

(t/use-fixtures :each fix/with-redis-keyspace)

(def ^:private prefix-k (pack/pack-hash-array 1 2))

(def ^:private test-data (concat (list (pack/pack-hash-array 0 1 2))
                                 (for [i (range 11)]
                                   (pack/pack-hash-array 1 2 i))
                                 (list (pack/pack-hash-array 1 3 1))))

(deftest get-range-test
  (proto/set-ks fix/*key-store* fix/*keyspace* test-data)
  (is (= 11 (count (proto/get-range fix/*key-store* fix/*keyspace* prefix-k))))
  (is (= 10 (count (proto/get-range fix/*key-store* fix/*keyspace* prefix-k (pack/pack-hash-array 1 2 10)))))
  (is (= 5 (count (proto/get-range fix/*key-store* fix/*keyspace* (pack/pack-hash-array 1 2 5) (pack/pack-hash-array 1 2 10)))))
  (is (= 3 (count (proto/get-range fix/*key-store* fix/*keyspace* (pack/pack-hash-array 1 2 5) (pack/pack-hash-array 1 2 10) 3))))
  (is (= 6 (count (proto/get-range fix/*key-store* fix/*keyspace* (pack/pack-hash-array 1 2 5) (pack/inc-ba (pack/copy prefix-k))))))
  (is (= 3 (count (proto/get-range fix/*key-store* fix/*keyspace* (pack/pack-hash-array 1 2 5) (pack/inc-ba (pack/copy prefix-k)) 3)))))

(deftest seek-test
  (proto/set-ks fix/*key-store* fix/*keyspace* test-data)
  (is (= 11 (count (proto/seek fix/*key-store* fix/*keyspace* prefix-k))))
  (is (= 10 (count (proto/seek fix/*key-store* fix/*keyspace* prefix-k 10))))
  (is (= 6 (count (proto/seek fix/*key-store* fix/*keyspace*  prefix-k (pack/int->bytes 5)))))
  (is (= 5 (count (proto/seek fix/*key-store* fix/*keyspace*  prefix-k (pack/int->bytes 5) 5))))
  (is (= 6 (count (proto/seek fix/*key-store* fix/*keyspace*  prefix-k (pack/int->bytes 5) 10)))))

(def ^:private empty-prefix (byte-array 0))

(def ^:private empty-prefix-test-data (for [i (range 11)] (pack/pack-hash-array i)))

(deftest seek-prefix-test
  (proto/set-ks fix/*key-store* fix/*keyspace* empty-prefix-test-data)
  (is (= 11 (count (proto/seek fix/*key-store* fix/*keyspace* empty-prefix))))
  (is (= 10 (count (proto/seek fix/*key-store* fix/*keyspace* empty-prefix 10))))
  (is (= 6 (count (proto/seek fix/*key-store* fix/*keyspace*  empty-prefix (pack/int->bytes 5)))))
  (is (= 5 (count (proto/seek fix/*key-store* fix/*keyspace*  empty-prefix (pack/int->bytes 5) 5))))
  (is (= 6 (count (proto/seek fix/*key-store* fix/*keyspace*  empty-prefix (pack/int->bytes 5) 10)))))
