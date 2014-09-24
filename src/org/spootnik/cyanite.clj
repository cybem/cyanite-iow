(ns org.spootnik.cyanite
  "Main cyanite namespace"
  (:gen-class)
  (:require [org.spootnik.cyanite.carbon :as carbon]
            [org.spootnik.cyanite.http   :as http]
            [org.spootnik.cyanite.config :as config]
            [clojure.tools.cli           :refer [cli]]))

(require 'beckon)

(set! *warn-on-reflection* true)

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

(defn -main
  "Our main function, parses args and launches appropriate services"
  [& args]
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
      (when (:enabled carbon)
        (carbon/start config))
      (when (:enabled http)
        (http/start config))))
  nil)
