(ns hooray.expression
  (:require [hooray.query.spec :as qs]))

(defn- resolve-symbol [s]
  (if (qualified-symbol? s)
    (requiring-resolve s)
    (ns-resolve 'clojure.core s)))

(defn find-logic-vars
  ([fn-call]
   (find-logic-vars fn-call []))
  ([{args :args :as fn-call} res]
   (reduce (fn [res [type arg]]
             (case type
               :fn-call (find-logic-vars arg res)
               :logic-var (conj res arg)
               :literal res))
           res args)))

(defn compile-expression )

(defn expr->fn [[_ {:keys [fn-call binding] :as original-expr}]]
  {:original-expr original-expr
   :input-args (find-logic-vars fn-call)})

(comment
  (compile-expression
   '[:expression
     {:fn-call
      {:fn *,
       :args
       [[:fn-call {:fn +, :args [[:logic-var a] [:logic-var b]]}]
        [:logic-var c]]},
      :binding d}]))
