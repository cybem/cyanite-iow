(ns org.spootnik.cyanite.rollup
  "Implements rollup finders for different strategies.")

(defprotocol RollupFinder
  (find-best-rollup [this from to]))

(defn get-within-fn
  [from]
  (fn [{:keys [rollup period] :as rollup-def}]
    (and (>= (Long/parseLong from) (- (now) (* rollup period)))
         rollup-def)))

(defn precise-rollup-finder
  [{:keys [rollups]}]
  (reify
    RollupFinder
    (find-best-rollup
      "Find most precise storage period given the oldest point wanted"
      [this from to]
      (let [within-fn (get-within-fn from)]
        (some within-fn (sort-by :rollup rollups))))))

(defn resolution-rollup-finder
  [{:keys [rollups resolution]}]
  (reify
    RollupFinder
    (find-best-rollup
      "Find most suitable rollup for given resolution"
      [this from to]
      (let [within-fn (get-within-fn from)
            sorted-rollups (sort-by :rollup rollups)
            within (filter within-fn sorted-rollups)
            resolution-fn (fn [{:keys [rollup period] :as rollup-def}]
                            (>= (/ (- to from) rollup) resolution))]
        (or (some resolution-fn (rseq within)) (first within))))))
