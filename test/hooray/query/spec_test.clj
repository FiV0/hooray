(ns hooray.query.spec-test
  (:require [hooray.query.spec :as qs]
            [clojure.test :as t]))

;; just test
(t/deftest conform-query-test
  (t/is map? (qs/conform-query '{:find [a b c]
                                 :where [[a :attr1 b]
                                         [b :attr2 c]
                                         [c :attr3 foo]
                                         [c :attr4 "bar"]
                                         [(* (+ a b) c) d] ;; expression with binding
                                         [(odd? d)]        ;; predicate call
                                         (not-join [e]
                                                   [e :bla :toto])
                                         (or-join [e]
                                                  (and [e :foo :bar]
                                                       [e :bar :foo])
                                                  [e :foo bar]
                                                  )
                                         ;; nesting
                                         (or-join [e]
                                                  (and [e :foo :bar]
                                                       [e :bar :foo])
                                                  [e :foo bar]
                                                  (not-join [e]
                                                            [e :foo :bar]))
                                         (not-join [e]
                                                   [e :foo bar]
                                                   (or-join [e]
                                                            (and [e :foo :bar]
                                                                 [e :bar :foo])
                                                            [e :foo bar]))]})))
