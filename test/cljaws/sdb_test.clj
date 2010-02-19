(ns cljaws.sdb-test
  (:use (cljaws core sdb core-test helpers) :reload-all)
  (:use [clojure.test]))

(def timeout-seconds 15)

(deftest list-domains-test
  (let [domain-name (make-unique-name "domain")]
    (with-aws 
      (with-sdb
	(create-domain domain-name)
	
	(let [result (list-domains)]
	  (is (seq? result))
	  (is (pos? (count result))
	      "Should get list of available domains")
	  (is (string? (first result))))
	
	(is (while-or-timeout 
	     false? timeout-seconds 
	     (contains-string? (list-domains) domain-name))
	    "Is this domain created?")
	
	(with-domain domain-name
	  (add-attributes :row1 {:color "red" :name "apple"})

	  (while-or-timeout 
	   false? timeout-seconds
	   (= 1 (count (select (str "* from `" domain-name "`")))))

	  (let [result (select (str  "* from `" domain-name "`") )]
	    (is (= 1 (count result)))
	    (is (= "red" (:color (second (first result)))))	  
	    (is (= "apple" (:name (second (first result)))))))
      
      
	(delete-domain domain-name)
      
	(is (while-or-timeout
	     false? timeout-seconds 
	     (not (contains-string? (list-domains) domain-name))))))))