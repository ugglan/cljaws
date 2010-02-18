(ns cljaws.helpers-test
  (:use [cljaws.helpers] :reload-all)
  (:use [clojure.test]))


(deftest timeout-test
  (is (= 2
	 (while-or-timeout zero? 1
			   2))
      "not zero, return value right away")

  (is (let [end-time (+ 2000 (System/currentTimeMillis))]
	(= 2
	   (while-or-timeout 
	    zero? 5
	    (if (< (System/currentTimeMillis) end-time) 0 2))))
      "not zero, return correct value after two seconds")
  
  (is (false?
       (while-or-timeout zero? 1
			 0))
      "keeps returning zero, return false after timeout")


  
  (is (let [start (System/currentTimeMillis)
	    total (do
		    (while-or-timeout zero? 2 0)
		    (- (System/currentTimeMillis) start))]
	(< 1900 total 2100))
      "timeout witinin 1.9s-2.1s interval"))
