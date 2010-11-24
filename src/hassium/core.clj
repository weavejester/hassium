(ns hassium.core
  (:require [hassium.predicates :as pred]
            [clojure.walk :as walk])
  (:import [com.mongodb Mongo DBCursor BasicDBObject]
           [org.bson.types ObjectId]
           [clojure.lang Counted IDeref Keyword Symbol]
           [java.util Map List Map$Entry]))

(declare *connection*)
(declare *database*)

(defn connect
  "Connect to a MongoDB server."
  [connection]
  (Mongo. (:host connection "127.0.0.1")
          (:port connection 27017)))

(defn database
  "Returns a MongoDB database object."
  [name]
  (.getDB *connection* name))

(defmacro with-connection
  "Evaluates its body in the context of the supplied MongoDB connection.
  The connection address is specified as a map, with keys for :host and
  :port."
  [address & body]
  `(binding [*connection* (connect ~address)]
     (binding [*database* (database (:database ~address))]
       (try ~@body
            (finally (.close *connection*))))))

(defprotocol AsMongo
  (as-mongo [x] "Turn x into a com.mongodb Java object."))

(extend-protocol AsMongo
  Map
  (as-mongo [m]
    (let [dbo (BasicDBObject.)]
      (doseq [[k v] m]
        (.put dbo (.replace (as-mongo k) \. \$)
                  (as-mongo v)))
      dbo))
  List
  (as-mongo [coll]
    (map as-mongo coll))
  Keyword
  (as-mongo [kw]
    (subs (str kw) 1))
  Symbol
  (as-mongo [s] (str s))
  Object
  (as-mongo [x] x)
  nil
  (as-mongo [_] nil))

(defprotocol AsClojure
  (as-clojure [x] "Turn x into a Clojure data structure"))

(extend-protocol AsClojure
  Map
  (as-clojure [m]
    (into {}
      (for [^Map$Entry e m]
        [(-> (.getKey e) (.replace \$ \.) keyword)
         (as-clojure (.getValue e))])))
  List
  (as-clojure [coll]
    (map as-clojure coll))
  ObjectId
  (as-clojure [id] (.toString id))
  Object
  (as-clojure [x] x)
  nil
  (as-clojure [_] nil))

(defrecord Collection [name]
  AsMongo
  (as-mongo [_]
    (.getCollection *database* name)))

(defn collection
  "Returns a MongoDB collection."
  [name]
  (Collection. name))

(defn- cursor-seq [^DBCursor cursor]
  (lazy-seq
    (if (.hasNext cursor)
      (cons (as-clojure (.next cursor))
            (cursor-seq cursor)))))

(deftype Cursor [make-cursor]
  AsMongo
  (as-mongo [_] (make-cursor))
  IDeref
  (deref [_] (cursor-seq (make-cursor)))
  Counted
  (count [_] (.count (make-cursor))))

(defmacro as-mongo->
  "Like ->, but wraps arguments of functions in as-mongo.
  e.g. (mongo-> cursor (.sort critera))
       => (.sort (as-mongo cursor) (as-mongo criteria))"
  [x & forms]
  `(-> (as-mongo ~x)
       ~@(for [f forms]
           (if (seq? f)
             (cons (first f)
                   (for [a (rest f)] `(as-mongo ~a)))
             f))))

(defn limit
  "Return a cursor limited to at most n results."
  [cursor n]
  (Cursor. #(as-mongo-> cursor (.limit n))))

(defn skip
  "Return a cursor that skips the first n results."
  [cursor n]
  (Cursor. #(as-mongo-> cursor (.skip n))))

(defn- sorting-order [fields]
  (apply merge
         (for [f fields]
           (if (map? f) f {f 1}))))

(defn order-by
  "Return a cursor ordered by the supplied fields."
  [cursor & fields]
  (let [order (sorting-order fields)]
    (Cursor. #(as-mongo-> cursor (.sort order)))))

(defn asc
  "Field to be sorted in ascending order."
  [field]
  {field 1})

(defn desc
  "Field to be sorted in descending order."
  [field]
  {field -1})

(defn save
  "Save the map into the collection. The inserted map is returned, with a
  generated :_id key if one has not been already set."
  [coll doc]
  (let [mongo-doc (as-mongo doc)]
    (.save (as-mongo coll) mongo-doc)
    (as-clojure mongo-doc)))

(defn insert
  "Insert each document map into the collection."
  [coll & docs]
  (doseq [doc docs] (save coll doc)))

(defn find-all
  "Find all documents in the collection matching the criteria."
  ([coll]
     (find-all coll {}))
  ([coll criteria]
     (find-all coll criteria nil))
  ([coll criteria fields]
     (Cursor. #(as-mongo-> coll (.find criteria fields)))))

(defn find-one
  "Find one document from the collection matching the criteria."
  ([coll]
     (find-one coll {}))
  ([coll criteria]
     (find-one coll criteria nil))
  ([coll criteria fields]
     (as-clojure (as-mongo-> coll (.findOne criteria fields)))))

(defn delete
  "Remove all documents matching the criteria."
  ([coll]
     (delete coll {}))
  ([coll criteria]
     (as-mongo-> coll (.remove criteria))))

(defmacro where
  "A small DSL for constructing a MongoDB query using S-expressions."
  [& clauses]
  `(merge
     ~@(walk/postwalk-replace
         {'=    `pred/eq
          'not= `pred/ne
          '>    `pred/gt
          '<    `pred/lt
          '>=   `pred/gte
          '<=   `pred/lte
          'not  `pred/not*}
         clauses)))
