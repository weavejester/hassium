(ns hassium.core
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

(defn order-by
  "Return a cursor ordered by the supplied criteria."
  [cursor criteria]
  (Cursor. #(as-mongo-> cursor (.sort criteria))))

(defn insert
  "Insert the supplied documents into the collection."
  [coll & docs]
  (doseq [doc docs]
    (as-mongo-> coll (.save doc))))

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
  [coll criteria]
  (as-mongo-> coll (.remove criteria)))

(with-connection {:database "mydb"}
  (let [coll (collection "testCollection")]
    (delete coll {})
    (insert coll
            {:foo "bar"}
            {:foo "baz"}
            {:foo "baa"})
    (prn @(-> (find-all coll) (limit 2)))
    (prn (-> (find-all coll) (count)))))
