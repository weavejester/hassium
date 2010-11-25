Hassium
=======

Hassium is a MongoDB client library for the Clojure programming
language.

Example
-------

    (def db
      (database "demo")) 
 
    (def people
      (collection db "people")

    (insert people {:name "Alice"}
                   {:name "Bob"}
                   {:name "Carol"})

    (doseq [person @(find-all people)]
      (println (:name person))))
