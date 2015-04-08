(ns org.spootnik.cyanite.config
  "Yaml config parser, with a poor man's dependency injector"
  (:import (java.net InetAddress))
  (:require [org.spootnik.cyanite.util :refer [set-aggregator-patterns!
                                               set-blacklist-patterns!
                                               nested-select-keys
                                               align-time]]
            [org.spootnik.cyanite.store_mware]
            [clj-yaml.core :refer [parse-string]]
            [clojure.string :refer [split]]
            [clojure.tools.logging :refer [error info debug]]))

(def
  ^{:doc "handle logging configuration from the yaml file"}
  default-logging
  {:use       "org.spootnik.cyanite.logging/start-logging"
   :pattern   "%p [%d] %t - %c - %m%n"
   :external  false
   :console   true
   :files     []
   :level     "info"
   :overrides {:org.spootnik "debug"}})

(def ^{:doc "handle storage with cassandra-metric-store by default"}
  default-store
  {:use "org.spootnik.cyanite.store/cassandra-metric-store"})

(def ^{:doc "let carbon listen on 2003 by default"}
  default-carbon
  {:enabled     true
   :host        "127.0.0.1"
   :port        2003
   :readtimeout 30
   :connecttimeout 1000})

(def ^{:doc "let the http api listen on 8080 by default"}
  default-http
  {:enabled true
   :host    "127.0.0.1"
   :port    8080})

(def ^{:doc "Send statistics every 60 seconds without tenant"}
  default-stats
  {:interval 60
   :hostname (.. InetAddress getLocalHost getHostName)
   :tenant   "NONE"
   :console  false})

(def default-index
  {:use "org.spootnik.cyanite.path/memory-pathstore"})

(def ^{:doc "Disabled by default. Get aggregation patterns from /etc/cyanite/aggregator.yaml"}
  default-aggregator
  {:enabled false
   :path    "/etc/cyanite/aggregator.yaml"})

(def ^{:doc "Disabled by default. Get blacklist patterns from /etc/cyanite/blacklist.yaml"}
  default-blacklist
  {:enabled false
   :path "/etc/cyanite/blacklist.yaml"})

(def default-store-middleware
  {:use "org.spootnik.cyanite.store_mware/store-middleware"})

(def default-store-cache
  {:use "org.spootnik.cyanite.store_cache/in-memory-cache"})

(def default-rollup-finder
  {:use "org.spootnik.cyanite.rollup/precise-rollup-finder"})

(defn to-seconds
  "Takes a string containing a duration like 13s, 4h etc. and
   converts it to seconds"
  [s]
  (let [[_ value unit] (re-matches #"^([0-9]+)([a-z])$" s)
        quantity (Integer/valueOf value)]
    (case unit
      "s" quantity
      "m" (* 60 quantity)
      "h" (* 60 60 quantity)
      "d" (* 24 60 60 quantity)
      "w" (* 7 24 60 60 quantity)
      "y" (* 365 24 60 60 quantity)
      (throw (ex-info (str "unknown rollup unit: " unit) {})))))

(defn convert-shorthand-rollup
  "Converts an individual rollup to a {:rollup :period :ttl} tri"
  [rollup]
  (if (string? rollup)
    (let [[rollup-string retention-string] (split rollup #":" 2)
          rollup-secs (to-seconds rollup-string)
          retention-secs (to-seconds retention-string)]
      {:rollup rollup-secs
       :period (/ retention-secs rollup-secs)
       :ttl    (* rollup-secs (/ retention-secs rollup-secs))})
    rollup))

(defn convert-shorthand-rollups
  "Where a rollup has been given in Carbon's shorthand form
   convert it to a {:rollup :period} pair"
  [rollups]
  (map convert-shorthand-rollup rollups))

(defn assoc-rollup-to
  "Enhance a rollup definition with a function to compute
   the rollup of a point"
  [rollups]
  (map (fn [{:keys [rollup] :as rollup-def}]
         (assoc rollup-def :rollup-to #(align-time % rollup)))
       rollups))

(defn find-ns-var
  "Find a symbol in a namespace"
  [s]
  (try
    (let [n (namespace (symbol s))]
      (require (symbol n))
      (find-var (symbol s)))
    (catch Exception e
      (prn "Exception: " e))))

(defn instantiate
  "Find a symbol pointing to a function of a single argument and
   call it"
  [class config]
  (if-let [f (find-ns-var class)]
    (f config)
    (throw (ex-info (str "no such namespace: " class) {}))))

(defn get-instance
  "For dependency injected configuration elements, find build fn
   and call it"
  [{:keys [use] :as config} target]
  (debug "building " target " with " use)
  (instantiate (-> use name symbol) config))

(defn load-path
  "Try to find a pathname, on the command line, in
   system properties or the environment and load it."
  [path]
  (-> (or path
          (System/getProperty "cyanite.configuration")
          (System/getenv "CYANITE_CONFIGURATION")
          "/etc/cyanite/cyanite.yaml")
      slurp
      parse-string))

(defn load-aggregator-config [path]
  (try
    (info "Loading aggregator rules from: " path)
    (doseq [[k v] (parse-string (slurp path) false)] (set-aggregator-patterns! k v))))

(defn load-blacklist-config [path]
  (try
    (info "Loading blacklist rules from: " path)
    (doseq [[k v] (parse-string (slurp path) false)] (set-blacklist-patterns! k v))))

(defn config-instance
  [config entity default & [deps]]
  (let [settings (merge default (nested-select-keys config deps))]
    (-> config
        (update-in [entity] (partial merge settings))
        (update-in [entity] get-instance entity))))

(defn init
  "Parse yaml then enhance config"
  [path quiet?]
  (try
    (when-not quiet?
      (println "starting with configuration: " path))
    (-> (load-path path)
        (config-instance :logging default-logging)
        (update-in [:stats] (partial merge default-stats))
        (config-instance :store default-store)
        (update-in [:carbon] (partial merge default-carbon))
        (update-in [:carbon :rollups] convert-shorthand-rollups)
        (update-in [:carbon :rollups] assoc-rollup-to)
        (config-instance :store-cache default-store-cache)
        (config-instance :store-middleware default-store-middleware
                         [:store :store-cache :rollups])
        (config-instance :index default-index)
        (update-in [:http] (partial merge default-http))
        (update-in [:aggregator] (partial merge default-aggregator))
        (update-in [:blacklist] (partial merge default-blacklist))
        (config-instance :rollup-finder default-rollup-finder [:rollups]))
    (catch Exception e
      (info e "Config processing exception"))))
