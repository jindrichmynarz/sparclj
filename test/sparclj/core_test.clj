(ns sparclj.core-test
  (:require [sparclj.core :as sparql]
            [clojure.java.io :as io]
            [clojure.test :refer :all]))

(deftest format-binding
  (let [xsd (partial str sparql/xsd-ns)]
    (are [datatype content result] (= (sparql/format-binding datatype content) result)
         (xsd "boolean") "true" true
         (xsd "double") "1.23" 1.23
         (xsd "float") ".1e6" 100000.0
         (xsd "integer") "5" 5
         (xsd "long") "10" 10
         (xsd "string") "foo" "foo")))

(deftest render-template
  (let [template "ping_endpoint.mustache"
        data {:source-graph "http://placeholder"}]
    (is (= (sparql/render-template template data)
           (sparql/render-template (slurp (io/resource template)) data)))))
