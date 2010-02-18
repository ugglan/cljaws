(ns cljaws.ec2
  (:use (cljaws core helpers))
  (:import 
   (com.xerox.amazonws.ec2 Jec2 ImageDescription)))

;;
;; Elastic cloud / EC2
;;


(defn list-ec2-images [] 
  (let [ec2 (Jec2. *aws-id* *aws-key*)]
    (map bean (.describeImages ec2 '()))))
