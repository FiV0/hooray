(ns hooray.query.spec
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]))


;; big parts copied from xtdb and core2
(s/def ::logic-var
  (s/and simple-symbol?
         (comp #(str/starts-with? % "?") name)))

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

(comment
  (s/conform ::triple '[?t :track/album ?album])
  (s/conform ::triple '[_ :track/album ?album]))

(s/def ::term (s/or :triple ::triple))

(s/def ::where (s/coll-of ::term :kind vector?))

(s/def ::limit nat-int?)

(s/def ::query
  (s/keys :req-un [::find]
          :opt-un [::where ::limit]))

;; TODO maybe write custom IllegalArgumentException
(defn conform-query [query]
  (let [conformed-query (s/conform ::query query)]
    (when (s/invalid? conformed-query)
      (throw  (ex-info "Malformed query"
                       {:query query
                        :explain (s/explain-data ::query query)})))
    conformed-query))


(comment
  (conform-query '{:find [?name]
                   :where
                   [[?t :track/name "For Those About To Rock (We Salute You)"]
                    [?t :track/album ?album]
                    [?album :album/artist ?artist]
                    [?artist :artist/name ]
                    [_ :foo/bar]]
                   :limit 12})

  )
