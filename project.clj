(defproject sparclj "0.1.7"
  :description "A Clojure library for talking with SPARQL endpoints"
  :url "https://github.com/jindrichmynarz/sparclj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :scm {:name "git"
        :url "https://github.com/jindrichmynarz/sparclj"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                 [org.clojure/data.zip "0.1.2"]
                 [org.clojure/data.xml "0.2.0-alpha2"]
                 [clj-http "3.4.1"]
                 [slingshot "0.12.2"]
                 [commons-validator/commons-validator "1.5.1"]
                 [stencil "0.5.0"]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "0.9.0"]]}
             :test {:resource-paths ["test/resources"]}})
