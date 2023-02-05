(ns hooray.tuple
  (:require [hooray.util :as util]))

;; what we want
;; selection : TupleBag , pred -> TupleBag
;; projection : TupleBag , names -> TupleBag
;; concat : TupleBag , TupleBag -> TupleBag
;; add only one colunm : TupleBag , iterable -> TupleBag

(deftype Tuple [v]
  clojure.lang.Indexed
  (nth [_ i] (nth v i))
  (nth [_ _i _not-found] (util/unsupported-ex))
  clojure.lang.IPersistentVector
  (assocN [_ _index _object] (util/unsupported-ex))
  (cons [_ object] (->Tuple (conj v object)))
  (length [_] (count v))
  clojure.lang.Counted
  (count [_] (count v))
  java.lang.Object
  (toString [_] (str v)))

(defmethod print-method Tuple [t ^java.io.Writer w]
  (.write w "hooray.tuple.Tuple")
  (.write w (str t)))

(comment
  (def t (->Tuple [1 2 3]))
  (nth t 1)
  (count t)
  (conj t 4)
  (nth (conj t 4) 0)
  (nth (conj t 4) 3))

(deftype TupleBag [size name->idx tuples])
