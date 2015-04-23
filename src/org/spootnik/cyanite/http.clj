(ns org.spootnik.cyanite.http
  "Very simple asynchronous HTTP API, implements two
   routes: paths and metrics to query existing paths
   and retrieve metrics"
  (:use net.cgrand.moustache)
  ;(:use lamina.core)
  (:use aleph.http)
  (:use ring.middleware.json)
  (:use ring.middleware.params)
  (:use ring.middleware.keyword-params)
  (:require [ring.util.codec            :as codec]
            [org.spootnik.cyanite.store :as store]
            [org.spootnik.cyanite.rollup :as rollup]
            [org.spootnik.cyanite.path  :as path]
            [org.spootnik.cyanite.util  :refer [counter-inc! now
                                                process-too-many-paths-ex]]
            [cheshire.core              :as json]
            [clojure.string             :as str]
            [lamina.core                :refer [enqueue]]
            [clojure.string             :refer [lower-case]]
            [clojure.tools.logging      :refer [info error debug]]
            [org.spootnik.cyanite.util :as util]))

(defn wrap-local-params [handler params]
  "Adds additional parameters to request"
  (fn [request]
    (handler (assoc request :local-params params))))

(defn- log-request
  [title request]
  (info title (if-let [json-params (:json-params request)]
                json-params
                (:query-string request))))

(defn paths-handler [response-channel {{:keys [query tenant]}  :params
                                       {:keys [index]} :local-params
                                       :as request}]
  (debug "query now: " query)
  (log-request "Paths request:" request)
  (enqueue
    response-channel
    (try
      {:status 200
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string
               (path/prefixes index (or tenant "NONE") (if (str/blank? query) "*" query)))}
      (catch Exception e
        (let [{:keys [status body suppress?]} (ex-data e)]
          (when-not suppress?
            (error e "could not process request"))
          (util/process-too-many-paths-ex e)
          {:status (or status 500)
           :headers {"Content-Type" "application/json"}
           :body    (json/generate-string
                      (or body {:error (.getMessage e)}))})))))

(defn lookup-path
  [index tenant path]
  (path/lookup index tenant path))

(defn has-wildcard?
  [path]
  (if (re-find #"(\*|\[|\{)" path) true false))

(defn lookup-paths
  [index tenant paths]
  (let [paths (if (sequential? paths) paths [paths])]
    (if (and (= (count paths) 1) (has-wildcard? (first paths)))
      (flatten (pmap (partial lookup-path index tenant) paths))
      paths)))

(defn metrics-handler [response-channel
                       {{:keys [index store rollup-finder]} :local-params
                        {:keys [from to path agg tenant]} :params :as request}]
  (debug "fetching paths: " path)
  (log-request "Metrics request:" request)
  (enqueue
   response-channel
    (try
      (do
        (counter-inc! (keyword (str "tenants." tenant ".metrics_read")) 1)
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (let [to (if to (Long/parseLong (str to)) (now))
                     from (Long/parseLong (str from))]
                 (if-let [{:keys [rollup period]}
                          (rollup/find-rollup rollup-finder from to)]
                   (let [paths (lookup-paths index (or tenant "NONE") path)]
                     (store/fetch store (or agg "mean") paths (or tenant "NONE")
                                  rollup period from to))
                   (json/generate-string
                    {:step nil :from nil :to nil :series {}})))})
      (catch Exception e
        (let [{:keys [status body suppress?]} (ex-data e)]
          (when-not suppress?
            (error e "could not process request"))
          (util/process-too-many-paths-ex e)
          {:status (or status 500)
           :headers {"Content-Type" "application/json"}
           :body    (json/generate-string
                     (or body {:error (.getMessage e)}))})))))

(def handler
  (app
    ["ping"] {:get "OK"}
    ["metrics"] {:any (-> metrics-handler
                          (wrap-aleph-handler)
                          (wrap-keyword-params)
                          (wrap-json-params)
                          (wrap-params))}
    ["paths"] {:any (-> paths-handler
                        (wrap-aleph-handler)
                        (wrap-keyword-params)
                        (wrap-json-params)
                        (wrap-params))}
    [&] {:any (json/generate-string {:status "Error" :reason "Unknown action"})}))

(defn start
  "Start the API, handling each request by parsing parameters and
   routes then handing over to the request processor"
  [{:keys [http store-middleware rollup-finder index] :as config}]
  (start-http-server (wrap-ring-handler  (wrap-local-params handler {:store store-middleware
                                                                     :rollup-finder rollup-finder
                                                                     :index index})) http)
  nil)
