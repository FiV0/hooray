(ns user
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.tools.namespace.repl :as repl]
            [lambdaisland.classpath.watch-deps :as watch-deps]
            [unilog.config :as ul]))

(defn watch-deps!
  []
  (watch-deps/start! {:aliases [:dev :test]}))

(defn go []
  (watch-deps!))


(defn better-logging! []
  (ul/start-logging! {:external  false
                      :console   "%d{yyyy-MM-dd HH:mm:ss.SSS} %highlight(%-5level) %logger{36} - %msg%n"
                      :level     :info
                      :overrides {"hooray"                                     :debug}}))

(better-logging!)

(comment
  (repl/set-refresh-dirs (io/file "src") (io/file "dev"))
  (repl/refresh)
  (repl/clear)

  (watch-deps!))
