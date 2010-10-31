(ns hassium.core
  (:import [com.mongodb Mongo BasicDBObject]
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
  nil
  (as-clojure [_] nil))

(defn insert
  "Insert the supplied document maps into the MongoDB collection."
  [collection & documents]
  (doseq [doc documents]
    (.save collection (as-mongo document))))

(with-connection {:database "mydb"}
  (let [coll (collection "testCollection")]
    (insert coll {:foo "bar"})
    (prn (as-clojure (.findOne coll)))))

(def document
  (doto (BasicDBObject.)
    (.put "hello" "world")))
