(ns hassium.core
  (:import [com.mongodb Mongo BasicDBObject]))

(defn connect
  "Connect to a MongoDB server."
  [connection]
  (Mongo. (:host connection "127.0.0.1")
          (:port connection 27017)))

(declare *connection*)

(defmacro with-connection
  "Evaluates its body in the context of the supplied MongoDB connection.
  The connection address is specified as a map, with keys for :host and
  :port."
  [address & body]
  `(binding [*connection* (connect ~address)]
     (try
       ~@body
       (finally (.close *connection*)))))

(defn database
  "Returns a MongoDB database object."
  [name]
  (.getDB *connection* name))

(defn collection
  "Returns a MongoDB collection object."
  [db name]
  (.getCollection db name))

(with-connection {}
  (prn (-> (database "mydb")
           (collection "testCollection")
           (.findOne))))

(def document
  (doto (BasicDBObject.)
    (.put "hello" "world")))
