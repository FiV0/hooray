(ns build
  (:require [clojure.java.io :as io]
            [clojure.tools.build.api :as b]))

(def lib 'fiv0/hooray)
(def version (format "0.1.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file (format "target/%s-%s.jar" (name lib) version))
(def main 'scratch)

(defn clean
  "Cleans the target path."
  [_]
  (b/delete {:path "target"}))

(defn java
  "Compiles the java classes under `src/java`."
  [_]
  (b/javac {:src-dirs ["src/java"]
            :class-dir "target/classes"
            :basis basis
            ; ignore Unsafe warnings
            :javac-opts ["-XDignore.symbol.file"]}))

(defn java-pom
  "Creates a pom.xml file at the root of the repository for the java files.
  Useful when working on the java files with other tools integrating maven."
  [_]
  (let [pom-dir (io/file class-dir "META-INF/maven/" (namespace lib) (name lib))
        pom-xml (io/file pom-dir "pom.xml")
        pom-properties (io/file pom-dir "pom.properties")]
    (b/write-pom {:class-dir class-dir
                  :lib lib
                  :version version
                  :basis basis
                  :src-dirs ["src/java"]})
    (io/copy pom-xml (io/file "pom.xml"))
    (io/delete-file pom-xml)
    (io/delete-file pom-properties)
    nil))

(defn jar
  "Creates a jar file of the project."
  [_]
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis basis
                :src-dirs ["src/clj"]})
  (b/copy-dir {:src-dirs ["src/clj" "resources"]
               :target-dir class-dir})
  (java nil)
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))

(comment
  (clean nil)
  (java nil)
  (jar nil))
