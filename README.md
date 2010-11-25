Hassium
=======

Hassium is a MongoDB client library for the Clojure programming
language.

Install
-------

Add the following dependency to your project.clj file:

    [hassium "0.2.0"]

Example
-------

    (use 'hassium.core)

    (def db
      (database "demo")) 
 
    (def people
      (collection db "people"))

    (insert people {:name "Alice"}
                   {:name "Bob"}
                   {:name "Carol"})

    (doseq [person @(find-all people)]
      (println (:name person)))
