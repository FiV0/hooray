(ns hooray.logical-plan)

#_{:clj-kondo/ignore #{:unused-binding}}
(defmulti emit-expr
  (fn [ra-expr srcs]
    (:op ra-expr)))
