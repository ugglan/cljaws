(ns cljaws.core-test
  (:use [cljaws.core] :reload-all)
  (:use [clojure.test]))


(defn make-queue-name []
  (str "__CLJAWS_test-queue-" (rand-int 100000000)))

(defmacro queue-test [& body]
  `(let [queue# (make-queue-name)]
     (with-aws (with-sqs-queue queue#
		 (let [msg# (do ~@body)]
		   (delete-queue)
		   msg#)))))

(deftest test-queues
  (is (= "Hello world!"
	   (queue-test 
	    (enqueue "Hello world!")
	    (dequeue))))

    (is (nil?
	 (queue-test 
	  (dequeue 5)))))


(deftest test-queue-create-delete
  (let [queues (with-aws (list-sqs-queues))
	names (take 10 (repeatedly make-queue-name))]
    
    (is (< (count queues)
	   (with-aws 
	     (doall
	      (for [i (range (count names))]
		(let [q (nth names i)]
		  (with-sqs-queue q
		    (enqueue (str "Hello " i q))
		    ))))
	     (count (list-sqs-queues)))))
    
    (is (= 10 (- (count queues) (count (with-aws (list-sqs-queues))))))
    
    
    (with-aws 
      (doall
       (for [i (range (count names))]
	 (let [q (nth names i)]
	   (with-sqs-queue q 
	     (is (= (str "Hello " i q)
		    (dequeue 20)))
	     (delete-queue))))))
    
    (is (= queues (with-aws (list-sqs-queues))))))

