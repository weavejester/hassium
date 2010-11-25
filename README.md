Hassium
=======

Hassium is a MongoDB client library for the Clojure programming
language.

Example
-------

    (def db 
      {:host "127.0.0.1"
       :port 27017
       :database "demo-db"})

    (def people
      (collection "people")

    (with-connection db
      (insert people {:name "Alice"}
                     {:name "Bob"}
                     {:name "Carol"})
      (doseq [p @(find-all people)]
        (println (:name p))))
