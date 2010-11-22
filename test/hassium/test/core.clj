(ns hassium.test.core
  (:use [hassium.core] :reload)
  (:use [clojure.test]))

(def db {:database "test"})

(def examples (collection "qexamples"))

(defn clear-examples [func]
  (with-connection db
    (delete examples)
    (func)))

(defn remove-ids [docs]
  (for [d docs] (dissoc d :_id)))

(use-fixtures :each clear-examples)

(deftest insertion-test
  (let [docs [{:foo "bar"} {:foo "baz"}]]
    (apply insert examples docs)
    (is (= (remove-ids @(find-all examples))
           docs))))

(deftest find-one-test
  (let [docs [{:foo "bar"} {:foo "baz"}]]
    (apply insert examples docs)
    (is (= (-> (find-one examples {:foo "baz"})
               (dissoc :_id))
           {:foo "baz"}))))
