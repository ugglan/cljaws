(ns cljaws.s3-test
  (:use (cljaws s3 core core-test helpers) :reload-all)
  (:use [clojure.test]))

(deftest list-buckets-test
  (let [bucket-name (make-unique-name "bucket")]
    (with-aws 
      (with-s3 
	(create-bucket bucket-name)
	
	(let [result (list-buckets)]
	  (is (seq? result))
	  (is (pos? (count result))
	      "Should get list of available bucketsimages")
	  (is (string? (first result))))
	
	(is (while-or-timeout 
	     false? 5 
	     (contains-string? (list-buckets) bucket-name)))

	(delete-bucket bucket-name)

	(is (while-or-timeout
	     false? 5 
	     (not (contains-string? (list-buckets) bucket-name))))))))