(ns hooray.expression
  (:require [hooray.query.spec :as qs]))

(defn- resolve-symbol [s]
  (if (qualified-symbol? s)
    (requiring-resolve s)
    (ns-resolve 'clojure.core s)))

(defn find-logic-vars
  ([fn-call]
   (find-logic-vars fn-call []))
  ([{args :args :as _fn-call} res]
   (reduce (fn [res [type arg]]
             (case type
               :fn-call (find-logic-vars arg res)
               :logic-var (conj res arg)
               :literal res))
           res args)))

(defn compile-expression [{fn-symbol :fn args :args :as original-expr}]
  (let [input-args (find-logic-vars original-expr)
        #_#_pos->input-args (zipmap (range) input-args)
        sub-calls (map (fn [[type arg]]
                         (case type
                           :fn-call (compile-expression arg)
                           :logic-var {:expr-fn identity
                                       :input-args [arg]}
                           :literal {:expr-fn (constantly arg)
                                     :input-args []}))
                       args)
        sub-res (repeatedly (count sub-calls) #(gensym "sub-res"))]
    {:expr-fn (eval `(fn ~input-args
                       (let ~(->>
                              (map (fn [{:keys [expr-fn input-args]}]
                                     `(~expr-fn ~@input-args)) sub-calls)
                              (interleave sub-res)
                              vec)
                         (~(resolve-symbol fn-symbol) ~@sub-res))))
     :input-args input-args}))

(defn expr->fn [{:keys [fn-call binding] :as original-expr}]
  (cond-> {:original-expr original-expr
           :compiled-expr (compile-expression fn-call)}
    binding (assoc :binding binding)))

(comment
  (-> (expr->fn
       '{:fn-call
         {:fn *,
          :args
          [[:fn-call {:fn +, :args [[:logic-var a] [:logic-var b]]}]
           [:logic-var c]]},
         :binding d})
      :compiled-expr
      :expr-fn
      (apply [2 2 3]))

  (-> (expr->fn
       '{:fn-call
         {:fn map,
          :args
          [[:logic-var a]
           [:fn-call {:fn map, :args [[:logic-var b] [:logic-var c]]}]]},
         :binding d})
      :compiled-expr
      :expr-fn
      (apply [inc inc [1 2 3]])))
