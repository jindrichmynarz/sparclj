(defproject sparclj "0.1.0-SNAPSHOT"
  :description "A Clojure library for talking with SPARQL endpoints"
  :url "https://github.com/jindrichmynarz/sparclj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                 [org.clojure/data.zip "0.1.2"]
                 [org.clojure/data.xml "0.0.8"]
                 [clj-http "3.4.1"]
                 [slingshot "0.12.2"]
                 [commons-validator/commons-validator "1.5.1"]
                 [stencil "0.5.0"]])
