(ns org.spootnik.cyanite
  "Main cyanite namespace"
  (:gen-class)
  (:require [org.spootnik.cyanite.carbon      :as carbon]
            [org.spootnik.cyanite.http        :as http]
            [org.spootnik.cyanite.config      :as config]
            [clojure.tools.cli                :refer [cli]]
            [clojure.tools.logging :refer [error info debug]]
            [beckon]
            [org.spootnik.cyanite.es-path]
            [org.spootnik.cyanite.store :as store]
            [org.spootnik.cyanite.store_cache :as cache]))

(set! *warn-on-reflection* true)

(def ^:const shutdown-wait-time 5)

(defn get-cli
  "Call cli parsing with our known options"
  [args]
  (try
    (cli args
         ["-h" "--help" "Show help" :default false :flag true]
         ["-f" "--path" "Configuration file path" :default nil]
         ["-q" "--quiet" "Suppress output" :default false :flag true])
    (catch Exception e
      (binding [*out* *err*]
        (println "Could not parse arguments: " (.getMessage e)))
      (System/exit 1))))

(defn shutdown
  [carbon-handle store store-middleware]
  (carbon/stop carbon-handle)
  (when store-middleware
    (store/shutdown store-middleware))
  (store/shutdown store)
  (info "Shutting down agents")
  (shutdown-agents)
  (info (format "Sleeping for %s seconds" shutdown-wait-time))
  (Thread/sleep (* shutdown-wait-time 1000))
  (info "Exiting")
  (System/exit 0))

(defn install-flusher
 [{:keys [store store-middleware]} carbon-handle]
 (reset! (beckon/signal-atom "TERM")
         [#(shutdown carbon-handle store store-middleware)]))

(defn -main
  "Our main function, parses args and launches appropriate services"
  [& args]
  (try
    (let [[{:keys [path help quiet]} args banner] (get-cli args)]
      (when help
        (println banner)
        (System/exit 0))
      (let [{:keys [carbon http aggregator blacklist] :as config} (config/init path quiet)]
        (let [load-configs
              (fn []
                (when (:enabled aggregator)
                  (config/load-aggregator-config (:path aggregator)))
                (when (:enabled blacklist)
                  (config/load-blacklist-config (:path blacklist))))]
          (reset! (beckon/signal-atom "HUP") #{load-configs})
          (load-configs))
        (install-flusher config (when (:enabled carbon) (carbon/start config)))
        (when (:enabled http)
          (http/start config))))
    nil
    (catch Exception e
      (error "Unhandled exception:" e)
      (System/exit 1))))
