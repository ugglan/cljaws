(ns cljaws.core-test
  (:use (cljaws core) :reload-all)
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


