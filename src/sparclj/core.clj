(ns sparclj.core
  (:require [sparclj.spec :as spec]
            [sparclj.xml-schema :as xsd]
            [clj-http.client :as client]
            [slingshot.slingshot :refer [throw+ try+]]
            [clojure.data.xml :as xml]
            [clojure.zip :as zip]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.spec :as s]
            [stencil.core :as stencil]))

; ----- Specs -----

(s/def ::auth (s/cat :username string?
                     :password string?))

(s/def ::max-retries (s/and int? pos?))

(s/def ::retries ::spec/non-negative-int)

(s/def ::sleep ::spec/non-negative-int)

(s/def ::update? boolean?)

(s/def ::url (s/and ::spec/iri spec/http?))

(s/def ::virtuoso? boolean?)

(s/def ::endpoint (s/keys :req [::url]
                          :opt [::auth ::max-retries ::sleep ::virtuoso?]))

(s/def ::query-args (s/cat :endpoint ::endpoint
                           :query string?))

; ----- Public vars -----

(def xsd "http://www.w3.org/2001/XMLSchema#")

; ----- Private functions -----

(s/fdef xml-schema->data-type
        :args ::spec/iri-urn
        :ret (s/or :string ::spec/iri-urn
                   :keyword ::spec/xsd-data-type))
(defn xml-schema->data-type
  "Coerce a XML Schema `data-type`."
  [data-type]
  (if-let [xsd-data-type (and (string/starts-with? data-type xsd)
                              (find-keyword "sparclj.xml-schema" (string/replace data-type xsd "")))]
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
  (Integer/parseInt content))

(defmethod format-binding ::xsd/long
  [_ content]
  (Long/parseLong content))

(defmethod format-binding :default
  [_ content]
  content)

(defn- get-binding
  "Get binding from `result`."
  [result]
  (let [{{:keys [datatype]} :attrs
         [content & _] :content
         :keys [tag]} (zip-xml/xml1-> result zip/down zip/node)]
    (if (and (= tag :literal) datatype)
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
                     :sparql-string string?
                     :opts (s/? (s/keys :opt [::update?]))))
(defn- execute-sparql
  [{::keys [auth sleep url virtuoso?]
    :or {sleep 0}
    :as endpoint}
   sparql-string
   & {::keys [update?]}]
  (let [[http-fn params-key] (if update? [client/post :form-params] [client/get :query-params])
        params (cond-> {params-key {"query" (prefix-virtuoso-operation virtuoso? sparql-string)}
                        :throw-entire-message? true}
                 (not update?) (assoc :headers {"Accept" "application/sparql-results+xml"})
                 auth (assoc :digest-auth auth))]
    (when-not (zero? sleep) (Thread/sleep sleep))
    (try+ (let [response (http-fn url params)]
            (if (and virtuoso? (incomplete-results? response))
              (throw+ {:type ::incomplete-results})
              (:body response)))
          (catch [:status 401] _
            (throw+ {:type ::invalid-auth}))
          (catch [:status 404] _
            (throw+ {:type ::endpoint-not-found})))))

(s/fdef execute-query
        :args (s/cat :endpoint ::endpoint
                     :query string?
                     :opts (s/? (s/keys :opt [::retries]))))
(defn- execute-query
  "Execute SPARQL `query`."
  [{::keys [max-retries]
    :or {max-retries 0}
    :as endpoint}
   query
   & {::keys [retries]
      :or {retries 0}}]
  (try+ (execute-sparql endpoint query)
        (catch [:type ::incomplete-results] exception
          (if (< retries max-retries)
            (do (Thread/sleep (+ (* retries 1000) 1000))
                (execute-query endpoint query ::retries (inc retries)))
            (throw+ exception)))))

(def ^:private extract-query-results
  "Extract results from SPARQL Query Results XML Format"
  (comp :content second :content xml/parse-str)) 

(defn extract-update-response
  "Extract response from SPARQL Update operation."
  [response]
  (-> response
      xml/parse-str
      zip/xml-zip
      (zip-xml/xml1-> :sparql :results :result :binding zip-xml/text)))

; ----- Public functions -----

(s/fdef ask-query
        :args ::query-args
        :ret boolean?)
(defn ask-query
  "Execute SPARQL ASK `query` on `endpoint`."
  [endpoint query]
  (->> query
       (execute-query endpoint)
       extract-query-results
       first
       Boolean/parseBoolean))

(s/fdef select-query
        :args ::query-args)
(defn select-query
  "Execute SPARQL SELECT `query` on `endpoint`.
  Returns an empty sequence when the query has no results."
  [endpoint query]
  (doall (for [result (extract-query-results (execute-query endpoint query))
               :let [zipper (zip/xml-zip result)]]
           (->> (zip-xml/xml-> zipper :binding variable-binding-pair)
                (partition 2)
                (map vec)
                (into {})))))

(s/fdef select-template
        :args (s/cat :endpoint ::endpoint
                     :template string?
                     :data (s/? map?)))
(defn select-template
  "Execute SPARQL SELECT query rendered from Mustache `template` file using `data`."
  [endpoint
   template
   & {:keys [data]
      :or {data {}}}]
  (select-query endpoint (render-template template data)))

(s/fdef update-operation
        :args (s/cat :endpoint ::endpoint
                     :operation string?))
(defn update-operation
  "Execute SPARQL Update `operation` on `endpoint`."
  [endpoint
   operation]
  (extract-update-response (execute-sparql endpoint operation ::update? true)))
