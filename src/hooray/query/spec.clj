;; This file initially contained some parts copied from XTDB
;; Copyright © 2018-2022 JUXT LTD.

(ns hooray.query.spec
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [hooray.util :as util])
  (:import (java.nio ByteBuffer)))

(defn logic-var? [v]
  (and (simple-symbol? v)
       #_(comp #(str/starts-with? % "?") name)))

(s/def ::logic-var logic-var?)

(s/def ::aggregate
  (s/cat :aggregate simple-symbol?
         :logic-var ::logic-var))

(s/def ::find-arg
  (s/or :logic-var ::logic-var
        :aggregate ::aggregate))

(s/def ::find (s/coll-of ::find-arg :kind vector? :min-count 1))

(def ^:private eid? (some-fn string? number? inst? keyword?))

(s/def ::eid eid?)
(s/def ::value any?)

(s/def ::source
  (s/and simple-symbol?
         (comp #(str/starts-with? % "$") name)))

(s/def ::wildcard
  (s/and simple-symbol?
         #(= "_" (name %))))

(s/def ::triple
  (s/and vector?
         (s/conformer identity vec)
         (s/cat
          :e (s/or :wildcard ::wildcard :literal ::eid, :logic-var ::logic-var)
          :a (s/? (s/or :wildcard ::wildcard :logic-var ::logic-var, :literal ::value)) ;; should this be just keyword?
          :v (s/? (s/or :wildcard ::wildcard :logic-var ::logic-var, :literal ::value)))))

(defn distinct-logic-vars? [triple]
  (apply distinct? (filter util/variable? triple)))


(s/def ::triple-unique
  (s/and distinct-logic-vars?
         ::triple))

(comment
  (s/conform ::triple '[?t :track/album ?album])
  (s/conform ::triple '[_ :track/album ?album])

  (s/valid? ::triple '[_ :track/album ?album])
  (s/valid? ::triple '[?a ?a ?b])
  (s/valid? ::triple-unique '[?a ?a ?b]))

(s/def ::triple-order
  (s/and (s/coll-of #{:e :a :v} :distinct true :kind vector? :min-count 1 :max-count 3)
         (s/conformer identity vec)))

(defn- expression-spec [sym spec]
  (s/and seq?
         #(= sym (first %))
         (s/conformer next (fn [v] [sym v]))
         spec))

(s/def ::args-list (s/coll-of ::logic-var :kind vector? :min-count 1))

(s/def ::and (expression-spec 'and (s/+ ::term)))

(s/def ::or-branches
  (s/+ (s/and (s/or :term ::term
                    :and ::and)
              (s/conformer (fn [[type arg]]
                             (case type
                               :term [arg]
                               :and arg))))))

(s/def ::or-join
  (expression-spec 'or-join (s/cat :args ::args-list
                                   :branches ::or-branches)))

(s/def ::not-join
  (expression-spec 'not-join (s/cat :args ::args-list
                                    :terms (s/+ ::term))))

(def ^:private built-ins '#{and})

;; need to restrict here to what we support
(s/def ::fn (s/and symbol? (complement built-ins)))

;; would be nice to support any kind of destructuring here
(s/def ::binding  any?)


(s/def ::fn-call (s/and seq?
                        (s/cat :fn ::fn
                               :args (s/* (s/or :fn-call ::fn-call
                                                :logic-var ::logic-var
                                                :literal ::value)))))

(s/def ::expression
  (s/and vector? (s/cat :fn-call ::fn-call
                        :binding (s/? ::binding))))

(s/def ::subquery
  (s/and seq?
         #(= 'q (first %))
         (s/conformer next (fn [v] (list 'q v)))
         ::query))

;; ranges probably need to tackled separately
(s/def ::term
  (s/or :triple ::triple
        :not-join ::not-join
        :or-join ::or-join
        :expression ::expression))

(s/def ::term-unique (s/or :triple ::triple-unique))

(s/def ::where (s/coll-of ::term :kind vector?))
(s/def :unique/where (s/coll-of ::term-unique :kind vector?))


(s/def ::limit nat-int?)

;; in

(s/def ::args-list (s/coll-of logic-var? :kind vector? :min-count 1))

(s/def ::binding
  (s/or :scalar logic-var?
        :collection (s/tuple logic-var? '#{...})
        :tuple ::args-list
        :relation (s/tuple ::args-list)))

(s/def ::in
  (s/and vector? (s/cat :source-var (s/? '#{$})
                        :bindings (s/* ::binding))))

(s/def ::query
  (s/keys :req-un [::find]
          :opt-un [::where ::limit ::in]))

(s/def ::query-unique
  (s/keys :req-un [::find]
          :opt-un [:unique/where ::limit ::in]))

(defn validate-query [query]
  (if (s/invalid? query)
    (throw  (ex-info "Malformed query"
                     {:query query
                      :explain (s/explain-data ::query query)}))
    query))

(defrecord ConformedQuery [query conformed-query])

;; TODO maybe write custom IllegalArgumentException
(defn conform-query
  ([query] (conform-query query {}))
  ([query {:keys [unique?] :as _opts}]
   (let [query-spec (if unique? ::query-unique ::query)
         conformed-query (s/conform query-spec query)]
     (when-let [map-keys ((some-fn :keys :syms :strs) query)]
       (when-not (= (count (:find query)) (count map-keys))
         (throw (IllegalArgumentException. "find and map-syntax arity mismatch!"))))
     (when (s/invalid? conformed-query)
       (throw  (ex-info "Malformed query"
                        {:query query
                         :explain (s/explain-data query-spec query)})))
     (->ConformedQuery query conformed-query))))

(defn wildcard? [v] (= v '_))

(defn wildcard-var [_] (gensym "?wildcard"))

(defn replace-wildcard [where-clause]
  (mapv #(cond-> % wildcard? (wildcard-var)) where-clause))

(defn triple->logic-vars [triple]
  (->> (vals triple)
       (filter (comp #{:logic-var} first))))

(comment
  (conform-query '{:find [?name]
                   :where
                   [[?t :track/name "For Those About To Rock (We Salute You)"]
                    [?t :track/album ?album]
                    [?album :album/artist ?artist]
                    [?artist :artist/name ]
                    [_ :foo/bar]]
                   :limit 12}))

(s/def ::tuple (s/and (s/keys :req-un [::triple ::triple-order])
                      (fn [{:keys [triple triple-order]}]
                        (= (count triple) (count triple-order)))))

(comment
  (s/valid? ::tuple {:triple '[?e :foo/bar]
                     :triple-order '[:e :a]})

  (def tuple (s/conform ::tuple {:triple '[?e :foo/bar]
                                 :triple-order '[:e :a]})))

(s/def ::byte-buffer #(instance? ByteBuffer %))
(s/def :persistent/triple
  (s/and vector?
         (s/conformer identity vec)
         (s/cat
          :e (s/or :literal ::byte-buffer, :logic-var ::logic-var)
          :a (s/? (s/or :logic-var ::logic-var, :literal ::byte-buffer))
          :v (s/? (s/or :logic-var ::logic-var, :literal ::byte-buffer)))))

(s/def ::persistent-tuple (s/and (s/keys :req-un [:persistent/triple ::triple-order])
                                 (fn [{:keys [triple triple-order]}]
                                   (= (count triple) (count triple-order)))))
