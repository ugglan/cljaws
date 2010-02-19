(ns cljaws.s3-test
  (:use (cljaws s3 core core-test helpers) :reload-all)
  (:use [clojure.test]))

(deftest buckets-test
  (let [bucket-name (make-unique-name "bucket")]
    (with-aws 
      (with-s3 
	(with-bucket bucket-name
	  
	  (let [result (list-buckets)]
	    (is (seq? result))
	    (is (pos? (count result))
		"Should get list of available bucketsimages")
	    (is (string? (first result))))
	
	  (is (while-or-timeout 
	       false? 5 
	       (contains-string? (list-buckets) bucket-name)))

	  (put-object "testing" "Hello World!")

	  (is (while-or-timeout
	       false? 10
	       (= "Hello World!" (get-object-as-string "testing"))))

	  (let [content (list-bucket)]
	    (is (= 1 (count content)))
	    (is (= "testing" (:key (first content)))))

	  (delete-object "testing")

	  (is (while-or-timeout 
	       false? 5
	       (zero? (count (list-bucket)))))

	  (delete-bucket))

	(is (while-or-timeout
	     false? 5 
	     (not (contains-string? (list-buckets) bucket-name))))))))