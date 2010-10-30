(ns hassium.core
  (:import [com.mongodb Mongo BasicDBObject]))

(def m (Mongo.))

(def db (.getDB m "mydb"))

(def coll (.getCollection db "testCollection"))

(def document
  (doto (BasicDBObject.)
    (.put "hello" "world")))

(.save coll document)

(prn (.findOne coll))
