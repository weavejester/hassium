(ns hassium.test.core
  (:use [hassium.core] :reload)
  (:use [clojure.test]))

(def db (database "test"))

(def people (collection db "people"))

(defn setup [func]
  (delete people)
  (insert people
    {:name "Alice", :sex "Female"}
    {:name "Bob",   :sex "Male"}
    {:name "Carol", :sex "Female"})
  (func))

(use-fixtures :each setup)

(deftest save-test
  (let [person (save people {:name "Dave"})]
    (is (contains? person :_id))
    (is (= (:name person) "Dave"))))

(deftest find-all-test
  (let [females @(find-all people {:sex "Female"})]
    (is (= (map :name females)
           ["Alice" "Carol"]))))

(deftest find-one-test
  (let [person (find-one people {:name "Alice"})]
    (is (= (:sex person)
           "Female"))))

(deftest count-test
  (is (= (count (find-all people)) 3)))

(deftest limit-test
  (is (= (map :name @(-> people find-all (limit 2)))
         ["Alice" "Bob"])))

(deftest skip-test
  (is (= (map :name @(-> people find-all (skip 1)))
         ["Bob" "Carol"])))

(deftest order-by-test
  (is (= (map :name @(-> people find-all (order-by :sex)))
         ["Alice" "Carol" "Bob"]))
  (is (= (map :name @(-> people find-all (order-by (desc :sex))))
         ["Bob" "Alice" "Carol"])))

(deftest delete-test
  (delete people {:name "Alice"})
  (is (= (map :name @(find-all people))
         ["Bob" "Carol"])))

(deftest where-test
  (is (= (where (= :x 1)) {:x 1}))
  (is (= (where (> :x 1)) {:x {:$gt 1}}))
  (is (= (where (not (< :x 1)))
         {:x {:$not {:$lt 1}}})))
