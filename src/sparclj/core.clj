(ns sparclj.core
  (:require [sparclj.spec :as spec]
            [sparclj.xml-schema :as xsd]
            [clj-http.client :as client]
            [slingshot.slingshot :refer [throw+ try+]]
            [clojure.zip :as zip]
            [clojure.data.xml :as xml]
            [clojure.data.zip :as zf]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [stencil.core :as stencil])
  (:import (java.net URL)))

; ----- Specs -----

(s/def ::accept string?)

(s/def ::auth (s/cat :username string?
                     :password string?))

(s/def ::max-retries (s/and int? pos?))

(s/def ::offset ::spec/positive-int)

(s/def ::page-size ::spec/positive-int)

(s/def ::parallel? boolean?)

(s/def ::proxy-host string?)

(s/def ::proxy-port ::spec/positive-int)

(s/def ::retries ::spec/non-negative-int)

(s/def ::sleep ::spec/non-negative-int)

(s/def ::update? boolean?)

(s/def ::url (s/and ::spec/iri spec/http?))

(s/def ::update-url ::url)

(s/def ::virtuoso? boolean?)

(s/def ::endpoint (s/keys :req [::url]
                          :opt [::auth ::max-retries ::page-size
                                ::proxy-host ::proxy-port
                                ::sleep ::update-url ::virtuoso?]))

(s/def ::query-args (s/cat :endpoint ::endpoint
                           :query string?))

; ----- Private vars -----

(def sparql-xml-mime "application/sparql-results+xml")

(def xsd-ns "http://www.w3.org/2001/XMLSchema#")

(xml/alias-uri :srx "http://www.w3.org/2005/sparql-results#")

; ----- Private functions -----

(s/fdef zipper?
        :args (s/cat :obj any?)
        :ret boolean?)
(defn- zipper?
  "Checks to see if the object has zip/make-node metadata on it (confirming it to be a zipper)."
  [obj]
  (contains? (meta obj) :zip/make-node))

(s/fdef xml-schema->data-type
        :args ::spec/iri-urn
        :ret (s/or :string ::spec/iri-urn
                   :keyword ::spec/xsd-data-type))
(defn xml-schema->data-type
  "Coerce a XML Schema `data-type`."
  [data-type]
  (if-let [xsd-data-type (and (string/starts-with? data-type xsd-ns)
                              (find-keyword "sparclj.xml-schema" (string/replace data-type xsd-ns "")))]
    xsd-data-type
    data-type))

(s/fdef format-binding
        :args (s/cat :data-type ::spec/data-type
                     :binding any?))
(defmulti format-binding
  "Format a SPARQL result binding"
  (fn [data-type _] (xml-schema->data-type data-type)))

(defmethod format-binding ::xsd/boolean
  [_ content]
  (Boolean/parseBoolean content))

(defmethod format-binding ::xsd/double
  [_ content]
  (Double/parseDouble content))

(defmethod format-binding ::xsd/float
  [_ content]
  (Float/parseFloat content))

(defmethod format-binding ::xsd/integer
  [_ content]
  (BigInteger. content))

(defmethod format-binding ::xsd/long
  [_ content]
  (Long/parseLong content))

(defmethod format-binding :default
  [_ content]
  content)

(def ^:private filter-elements
  "Filter element nodes from in node's children."
  (comp (partial filter (comp xml/element? zip/node)) zf/children))

(defn- get-binding
  "Get binding from `result`."
  [result]
  (let [{{:keys [datatype]} :attrs
         [content & _] :content
         :keys [tag]} (zip-xml/xml1-> result filter-elements zip/node)]
    (if (and (= tag ::srx/literal) datatype)
      (format-binding datatype content)
      content)))

(def ^:private variable-binding-pair
  "Returns a pair of variable name and its binding."
  (juxt (comp keyword (zip-xml/attr :name)) get-binding))

(defn- incomplete-results?
  "Test if `results` from Virtuoso indicate that they are incomplete."
  [results]
  (-> results
      (get-in [:headers "X-SQL-State"])
      (= "S1TAT")))

(s/fdef render-template
        :args (s/cat :template string?
                     :date map?)
        :ret string?)
(defn render-template
  "Render a Mustache template using data."
  [template data]
  (if (or (io/resource template) (io/resource (str template ".mustache")))
    (stencil/render-file template data)
    (stencil/render-string template data)))

(s/fdef prefix-virtuoso-operation
        :args (s/cat :virtuoso? ::virtuoso?
                     :sparql-string string?)
        :ret string?)
(defn- prefix-virtuoso-operation
  "Prefix `sparql-string` for Virtuoso if `virtuoso?` is true."
  [virtuoso?
   sparql-string]
  (if virtuoso?
    (str "DEFINE sql:log-enable 2\n" sparql-string)
    sparql-string))

(s/fdef execute-sparql
        :args (s/cat :endpoint ::endpoint
                     :sparql-fn fn?))
(defn- execute-sparql
  "Execute SPARQL on `endpoint`."
  [{::keys [sleep virtuoso?]
    :or {sleep 0}}
   sparql-fn]
  (when-not (zero? sleep) (Thread/sleep sleep))
  (try+ (let [response (sparql-fn)]
          (if (and virtuoso? (incomplete-results? response))
            (throw+ {:type ::incomplete-results})
            (:body response)))
        (catch [:status 401] _
          (throw+ {:type ::invalid-auth}))
        (catch [:status 404] _
          (throw+ {:type ::endpoint-not-found}))))

(s/fdef try-sparql
        :args (s/cat :endpoint ::endpoint
                     :sparql-fn fn?
                     :opts (s/keys* :opt [::retries])))
(defn- try-sparql
  "Try and potentially retry to execute a SPARQL query via `sparql-fn` on `endpoint`."
  [{::keys [max-retries]
    :or {max-retries 0}
    :as endpoint}
   sparql-fn
   & {::keys [retries]
      :or {retries 0}}]
  (try+ (execute-sparql endpoint sparql-fn)
        (catch [:type ::incomplete-results] exception
          (if (< retries max-retries)
            (do (Thread/sleep (+ (* retries 1000) 1000))
                (try-sparql endpoint
                            sparql-fn
                            ::retries (inc retries)))
            (throw+ exception)))))

(s/fdef execute-query
        :args (s/cat :endpoint ::endpoint
                     :sparql-string string?
                     :opt (s/keys* :req [::accept])))
(defn- execute-query
  "Execute SPARQL query `sparql-string` on SPARQL `endpoint."
  [{::keys [auth proxy-host proxy-port url virtuoso?]
    :as endpoint}
   sparql-string
   & {::keys [accept]}]
  (let [; Virtuoso expects text/plain MIME type for N-Triples.
        accept' (if (and virtuoso? (= accept "text/ntriples")) "text/plain" accept)
        base-params {:headers {"Accept" accept'}
                     :query-params {"query" sparql-string}
                     :throw-entire-message? true}
        params (cond-> base-params
                 auth (assoc :digest-auth auth)
                 proxy-host (assoc :proxy-host proxy-host
                                   :proxy-port proxy-port))
        sparql-fn (partial client/get url params)]
    (try-sparql endpoint sparql-fn)))

(s/fdef execute-update
        :args (s/cat :endpoint ::endpoint
                     :sparql-string string?))
(defn- execute-update
  "Execute SPARQL Update operation in `sparql-string` on SPARQL `endpoint`."
  [{::keys [auth proxy-host proxy-port update-url url virtuoso?]
    :as endpoint}
   sparql-string]
  (let [base-params {:form-params {"update" (prefix-virtuoso-operation virtuoso? sparql-string)}
                     :throw-entire-message? true}
        params (cond-> base-params
                 auth (assoc :digest-auth auth)
                 proxy-host (assoc :proxy-host proxy-host
                                   :proxy-port proxy-port))]
    (execute-sparql endpoint (partial client/post (or update-url url) params))))

(s/fdef extract-sparql-results
        :args (s/cat :results string?)
        :ret zipper?)
(def ^:private extract-sparql-results
  "Extract results from SPARQL Query Results XML Format"
  (comp zip/xml-zip xml/parse-str))

(s/fdef extract-ask-results
        :args (s/cat :results string?)
        :ret boolean?)
(defn- extract-ask-results
  "Extract results of a SPARQL ASK query"
  [results]
  (-> results
      extract-sparql-results
      (zip-xml/xml1-> ::srx/sparql ::srx/boolean zip-xml/text)
      Boolean/parseBoolean))

(s/fdef extract-select-results
       :args (s/cat :results string?)
       :ret zipper?)
(defn- extract-select-results
  "Extract results of a SPARQL SELECT query."
  [results]
  (-> results
      extract-sparql-results
      (zip-xml/xml-> ::srx/sparql ::srx/results ::srx/result)))

(s/fdef extract-update-results
       :args (s/cat :results string?)
       :ret string?)
(defn- extract-update-results
  "Extract response from SPARQL Update operation."
  [results]
  (-> results
      extract-sparql-results
      (zip-xml/xml1-> ::srx/sparql ::srx/results ::srx/result ::srx/binding zip-xml/text)))

(s/fdef lazy-cat'
        :args (s/cat :colls (s/coll-of seq?))
        :ret seq?)
(defn- lazy-cat'
  "Lazily concatenates a sequences `colls`.
  Taken from <http://stackoverflow.com/a/26595111/385505>."
  [colls]
  (lazy-seq
    (if (seq colls)
      (concat (first colls) (lazy-cat' (next colls))))))

(s/fdef get-http-proxy
        :args (s/cat)
        :ret (s/keys :req [::proxy-host ::proxy-port]))
(defn- get-http-proxy
  []
  (when-let [http-proxy (System/getenv "http_proxy")]
    (let [url (URL. http-proxy)]
      {::proxy-host (.getHost url)
       ::proxy-port (.getPort url)})))

; ----- Public functions -----

(s/fdef init-endpoint
        :args (s/cat :endpoint ::endpoint)
        :ret ::endpoint)
(defn init-endpoint
  "Initialize SPARQL endpoint to test if it is up and accessible."
  [{::keys [auth url]
    :as endpoint}]
  (try+ (let [params {:query-params {:query "ASK { [] ?p [] . }"}
                      :throw-entire-message? true}
              virtuoso? (-> url
                            (client/get (cond-> params
                                          auth (assoc :digest-auth auth)))
                            (get-in [:headers "Server"] "")
                            (string/includes? "Virtuoso"))]
          (assoc endpoint ::virtuoso virtuoso?))
        (catch [:status 401] _
          (throw+ {:type ::invalid-auth}))
        (catch [:status 404] _
          (throw+ {:type ::endpoint-not-found}))))

(s/fdef ask-query
        :args ::query-args
        :ret boolean?)
(defn ask-query
  "Execute SPARQL ASK `query` on `endpoint`."
  [endpoint query]
  (extract-ask-results (execute-query endpoint query ::accept sparql-xml-mime)))

(s/fdef construct-query
        :args (s/cat :endpoint ::endpoint
                     :query string?
                     :opts (s/keys* :opt [::accept]))
        :ret string?)
(defn construct-query
  "Execute SPARQL CONSTRUCT or DESCRIBE `query` on `endpoint`."
  [endpoint query & {::keys [accept]
                     :or {accept "text/turtle"}}]
  (execute-query endpoint query ::accept accept))

(s/fdef select-query
        :args ::query-args)
(defn select-query
  "Execute SPARQL SELECT `query` on `endpoint`.
  Returns an empty sequence when the query has no results."
  [endpoint query]
  (doall (for [result (extract-select-results (execute-query endpoint query ::accept sparql-xml-mime))]
           (->> (zip-xml/xml-> result ::srx/binding variable-binding-pair)
                (partition 2)
                (map vec)
                (into {})))))

(s/fdef select-template
        :args (s/cat :endpoint ::endpoint
                     :template string?
                     :data (s/? map?)))
(defn select-template
  "Execute SPARQL SELECT query rendered from Mustache `template` file using `data`."
  ([endpoint template]
   (select-template endpoint template {}))
  ([endpoint template data]
   (select-query endpoint (render-template template data))))

(s/fdef select-paged
        :args (s/cat :endpoint ::endpoint
                     :get-query-fn (s/fspec :args (s/cat :page (s/cat :limit ::page-size
                                                                      :offset ::offset)))
                     :opts (s/keys* :opt [::parallel?])))
(defn select-paged
  "Lazily execute paged SPARQL SELECT queries that are rendered from `get-query-fn`,
  which is passed `page-size` (LIMIT) and increasing OFFSET as [page-size offset]."
  [{::keys [page-size]
    :as endpoint}
   get-query-fn
   & {::keys [parallel?]}]
  (let [map-fn (if parallel? pmap map)
        pages (map vector (repeat page-size) (iterate (partial + page-size) 0))
        execute-query-fn (partial select-query endpoint)]
    (->> pages
         (map-fn (comp execute-query-fn get-query-fn))
         (take-while seq)
         lazy-cat')))

(s/fdef update-operation
        :args (s/cat :endpoint ::endpoint
                     :operation string?))
(defn update-operation
  "Execute SPARQL Update `operation` on `endpoint`."
  [endpoint
   operation]
  (extract-update-results (execute-update endpoint operation)))
