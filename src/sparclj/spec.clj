(ns sparclj.spec
  (:require [clojure.spec :as s])
  (:import (org.apache.commons.validator.routines UrlValidator)))

(def http?
  (partial re-matches #"^https?:\/\/.*$"))

(def urn?
  (partial re-matches #"(?i)^urn:[a-z0-9][a-z0-9-]{0,31}:[a-z0-9()+,\-.:=@;$_!*'%/?#]+$"))

(def valid-url?
  "Test if `url` is valid."
  (let [validator (UrlValidator. UrlValidator/ALLOW_LOCAL_URLS)]
    (fn [url]
      (.isValid validator url))))

(s/def ::iri (s/and string? valid-url?))

(s/def ::non-negative-int (s/and int? (complement neg?)))

(s/def ::urn (s/and string? urn?))

(s/def ::xsd-data-type (s/and keyword? (comp (partial = "sparclj.xml-schema") namespace)))

(s/def ::data-type (s/or :xsd-data-type ::xsd-data-type
                         :data-type ::iri-urn))

(s/def ::iri-urn (s/or :iri ::iri
                       :urn ::urn))
