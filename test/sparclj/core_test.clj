(ns sparclj.core-test
  (:require [sparclj.core :as sparql]
            [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [clojure.zip :as zip]))

; ----- Helper functions -----

(defn- parse-xml
  [xml]
  (-> xml
      (xml/parse-str :skip-whitespace true)
      zip/xml-zip))

(defn- wrap-binding
  [b]
  (format "<binding xmlns='http://www.w3.org/2005/sparql-results#'>%s</binding>" b))

; ----- Tests -----

(deftest format-literal
  (let [xsd (partial str sparql/xsd-ns)]
    (are [datatype content result] (= (sparql/format-literal datatype content) result)
         (xsd "boolean") "true" true
         (xsd "double") "1.23" 1.23
         (xsd "float") ".1e6" 100000.0
         (xsd "integer") "5" 5
         (xsd "long") "10" 10
         (xsd "string") "foo" "foo")))

(deftest get-binding
  (are [xml clj] (= clj (-> xml wrap-binding parse-xml sparql/get-binding))
       "<literal xml:lang='en'></literal>" ""
       "<literal/>" ""
       "<literal datatype='http://www.w3.org/2001/XMLSchema#integer'>30</literal>" 30))

(deftest render-template
  (let [template "ping_endpoint.mustache"
        data {:source-graph "http://placeholder"}]
    (is (= (sparql/render-template template data)
           (sparql/render-template (slurp (io/resource template)) data)))))

(deftest take-until
  (are [coll pred? result] (= (sparql/take-until pred? coll) result)
       [true false false] not [true false]
       [[0 1] [2] []] (comp (partial > 2) count) [[0 1] [2]]))
