(ns hassium.predicates
  "MongoDB predicates for the hassium.core/where macro.")

(defn eq [x y] {x y})
(defn ne [x y] {x {:$ne y}})

(defn gt [x y] {x {:$gt y}})
(defn lt [x y] {x {:$lt y}})
(defn gte [x y] {x {:$gte y}})
(defn lte [x y] {x {:$lte y}})

(defn not* [expr]
  {:pre [(map? expr)]}
  (let [[[k v]] (seq expr)]
    {k {:$not v}}))
