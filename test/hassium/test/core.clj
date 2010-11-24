(ns hassium.test.core
  (:use [hassium.core] :reload)
  (:use [clojure.test]))

(def db {:database "test"})

(def people (collection "people"))

(defn setup [func]
  (with-connection db
    (delete people)
    (insert people
      {:name "Alice", :sex "Female"}
      {:name "Bob",   :sex "Male"}
      {:name "Carol", :sex "Female"})
    (func)))

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

(deftest delete-test
  (delete people {:name "Alice"})
  (is (= (map :name @(find-all people))
         ["Bob" "Carol"])))

(deftest where-test
  (is (= (where (= :x 1)) {:x 1}))
  (is (= (where (> :x 1)) {:x {:$gt 1}}))
  (is (= (where (not (< :x 1)))
         {:x {:$not {:$lt 1}}})))
