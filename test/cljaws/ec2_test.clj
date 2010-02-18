(ns cljaws.ec2-test
  (:use (cljaws ec2 core helpers) :reload-all)
  (:use [clojure.test]))


(deftest list-images-test
  (let [result (with-aws (list-ec2-images))]
    
    (is (seq? result))
    (is (pos? (count result))
	"Should get list of available images")
    (is (contains? (first result) :imageId))))
