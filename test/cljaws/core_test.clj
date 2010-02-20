(ns cljaws.core-test
  (:use (cljaws core s3 sdb) :reload-all)
  (:use [clojure.test]))

;
; Test helpers
;

(defn contains-string? [coll s]
  (some #(= % s) coll))

(defn make-unique-name [type]
  (str "CLJAWS-test-" type  "-" (System/currentTimeMillis) (rand-int 100000000)))

(defmacro probe 
  "Print out an optional label and the result from body and then return it unaffected."
  {:arglists '([label? & body])}
  [& body]
  (let [f (first body)
	label (if (string? f) (str " (" f ")") "")
	body (if (string? f) (rest body) body)]
    `(let [result# (do ~@body)]
       (println "Probe" ~label ": " result#)
       result#)))


;
;
;


(defmacro unbound? [symbol]
  `(= :unbound 
      (try ~symbol
	   (catch IllegalStateException e# :unbound))))


(deftest compact-style-test
  (is (unbound? *aws-id*))
  (with-aws
    (is (string? *aws-id*)))
  
  (is (unbound? cljaws.s3/*s3-service*))

  (with-aws s3
    (is (not (unbound? cljaws.s3/*s3-service*))))
  
  (is (unbound? cljaws.s3/*s3-service*))
  (is (unbound? cljaws.sdb/*sdb-service*))
  
  (with-aws s3 sdb 
    (is (not (unbound? cljaws.s3/*s3-service*)))
    (is (not (unbound? cljaws.sdb/*sdb-service*)))))
