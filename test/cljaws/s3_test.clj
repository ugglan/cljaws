(ns cljaws.s3-test
  (:use (cljaws s3 core core-test helpers) :reload-all)
  (:use [clojure.test]))

(def timeout-seconds 15)

(deftest buckets-test
  (let [bucket-name (make-unique-name "bucket")]
    (with-aws 
      (with-s3 
	(with-bucket bucket-name
	  
	  (let [result (list-buckets)]
	    (is (seq? result))
	    (is (pos? (count result))
		"Should get list of available buckets")
	    (is (string? (first result))))
	
	  (is (while-or-timeout 
	       false? timeout-seconds 
	       (contains-string? (list-buckets) bucket-name)))

	  (put-object "testing" "Hello World!")

	  (is (while-or-timeout
	       false? timeout-seconds
	       (= "Hello World!" (get-object-as-string "testing"))))

	  (let [content (list-bucket)]
	    (is (= 1 (count content)))
	    (is (= "testing" (:key (first content)))))

	  (delete-object "testing")

	  (is (while-or-timeout 
	       false? timeout-seconds
	       (zero? (count (list-bucket)))))

	  (delete-bucket))

	(is (while-or-timeout
	     false? timeout-seconds
	     (not (contains-string? (list-buckets) bucket-name))))))))