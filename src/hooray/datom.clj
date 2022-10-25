;; Copyright © 2016-2021 Cisco Systems Copyright © 2015-2022 Paula Gearon
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "Encapsulates the implementation of the Datom type"
      :author "Paula Gearon"}
    hooray.datom
  (:import [clojure.lang Associative Indexed Seqable]
           [java.io Writer]))

(defprotocol Vectorizable
  (as-vec [o] "Converts object to a vector"))

(declare ->Datom)

;; simple type to represent the assertion or removal of data
(deftype Datom [e a v tx added]
  Vectorizable
  (as-vec [_] [e a v tx added])

  Associative
  (containsKey [_ i] (if (int? i)
                       (and (<= 0 i) (< i 5))
                       (#{:e :a :v :tx :added} i)))
  (entryAt [this i] (nth (as-vec this) i))
  (assoc [this k v] (apply ->Datom (assoc (as-vec this) ({:e 0 :a 1 :v 2 :tx 3 :added 4} k k) v)))
  (count [_] 5)
  (cons [this c] (cons c (as-vec this)))
  (empty [_] (throw (ex-info "Unsupported Operation" {})))
  (equiv [this o] (= (as-vec this) (as-vec o)))
  (valAt [this n] (nth (as-vec this) n))
  (valAt [this n not-found] (if (and (int? n) (<= 0 n) (< n 5)) (nth (as-vec this) n) not-found))
  Indexed
  (nth [this n] (nth (as-vec this) n))
  (nth [this n not-found] (if (and (int? n) (<= 0 n) (< n 5)) (nth (as-vec this) n) not-found))
  Seqable
  (seq [this] (seq (as-vec this)))

  Object
  (toString [this]
    (let [data (prn-str [e a v tx (if added :db/add :db/retract)])]
      (str "#datom " data))))

(defn datom-reader
  [[e a v tx added]]
  (->Datom e a v tx (= added :db/add)))

(defmethod clojure.core/print-method hooray.datom.Datom [o ^Writer w]
  (.write w "#datom ")
  (print-method (as-vec o) w))

(defmethod clojure.core/print-dup hooray.datom.Datom [o ^Writer w]
  (.write w "#hooray/datom ")
  (print-method (as-vec o) w))
