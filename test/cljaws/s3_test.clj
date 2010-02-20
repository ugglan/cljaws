(ns cljaws.s3-test
  (:use (cljaws s3 core core-test helpers) :reload-all)
  (:use [clojure.test] 
	[clojure.contrib.duck-streams]))

(def timeout-seconds 15)


(deftest buckets-test
  (let [bucket-name (make-unique-name "bucket")]
    (with-aws :s3
	; create bucket
	(with-bucket bucket-name

	  ; verify that it exists
	  (is (while-or-timeout 
	       false? timeout-seconds 
	       (contains-string? (list-buckets) bucket-name)))

	  ; put a simple textobject
	  (put-object "testing" "Hello World!")

	  ; verify it exists
	  (is (while-or-timeout
	       false? timeout-seconds
	       (= "Hello World!" (get-object-as-string "testing"))))

	  (let [content (list-bucket)]
	    (is (= 1 (count content)))
	    (is (= "testing" (:key (first content)))))


	  ; create a file and put it
	  (let [file (java.io.File/createTempFile "CLJAWS_test" ".html")
		test-content "<html><head><title>Hello</title></head>
<body><h1>Hello world!</h1></body></html>"]
	    
	    (spit file test-content)

	    (put-object "test.html" file)

	  ; verify it exists
	    (is (while-or-timeout
		 false? timeout-seconds
		 (= "test.html" (:key (get-object-details "test.html")))))

	    (let [content (list-bucket)]
	      (is (= 2 (count content))))

	    ; delete them
	    (delete-object "test.html"))
	  (delete-object "testing")
	  
	  ; verify they're gone
	  (is (while-or-timeout 
	       false? timeout-seconds
	       (zero? (count (list-bucket)))))

	  ; delete bucket
	  (delete-bucket))

	; verify the bucket is gone
	(is (while-or-timeout
	     false? timeout-seconds
	     (not (contains-string? (list-buckets) bucket-name)))))))

(deftest fail-test
    (let [bucket-name (make-unique-name "bucket")]
    (with-aws :s3
      (with-bucket bucket-name
	(is (nil? (delete-object "This_doesnt_exist")))
	(is (nil? (get-object-details "This_doesnt_exits_either")))
	(is (nil? (get-object "This_doesnt_exits_either")))
	(is (nil? (get-object-as-string "This_doesnt_exits_either")))
	(is (nil? (get-object-as-stream "This_doesnt_exits_either")))
	(delete-bucket)))))