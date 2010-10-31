(ns hassium.core
  (:refer-clojure :exclude [find remove])
  (:import [com.mongodb Mongo DBCursor BasicDBObject]
           [org.bson.types ObjectId]
           [clojure.lang Keyword Symbol]
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

(defn collection
  "Returns a MongoDB collection object."
  ([name]    (collection *database* name))
  ([db name] (.getCollection db name)))

(defprotocol AsMongo
  (as-mongo [x] "Turn x into a com.mongodb.DBObject"))

(extend-protocol AsMongo
  Map
  (as-mongo [m]
    (let [dbo (BasicDBObject.)]
      (doseq [[k v] m]
        (.put dbo (name k) (as-mongo v)))
      dbo))
  List
  (as-mongo [coll]
    (map as-mongo coll))
  Keyword
  (as-mongo [kw] (name kw))
  Symbol
  (as-mongo [s] (name s))
  String
  (as-mongo [s] s)
  nil
  (as-mongo [_] nil))

(defprotocol AsClojure
  (as-clojure [x] "Turn x into a Clojure data structure"))

(extend-protocol AsClojure
  Map
  (as-clojure [m]
    (into {}
      (for [^Map$Entry e m]
        [(keyword (.getKey e))
         (as-clojure (.getValue e))])))
  List
  (as-clojure [coll]
    (map as-clojure coll))
  String
  (as-clojure [s] s)
  ObjectId
  (as-clojure [id] (.toString id))
  DBCursor
  (as-clojure [^DBCursor cursor]
    (lazy-seq
      (if (.hasNext cursor)
        (cons (as-clojure (.next cursor))
              (as-clojure cursor)))))
  nil
  (as-clojure [_] nil))

(defn insert
  "Insert the supplied documents into the collection."
  [coll & docs]
  (doseq [doc docs]
    (.save coll (as-mongo doc))))

(defn find
  "Find all documents in the collection matching the criteria."
  ([coll]
     (as-clojure (.find coll)))
  ([coll criteria]
     (as-clojure (.find coll (as-mongo criteria)))))

(defn find-one
  "Find one document from the collection matching the criteria."
  ([coll]
     (as-clojure (.findOne coll)))
  ([coll criteria]
     (as-clojure (.findOne coll (as-mongo criteria)))))

(defn remove
  "Remove all documents matching the criteria."
  [coll criteria]
  (.remove coll (as-mongo criteria)))

(with-connection {:database "mydb"}
  (let [coll (collection "testCollection")]
    (remove coll {:hello "world"})
    (remove coll {:foo "bar"})
    (prn (find coll))))
