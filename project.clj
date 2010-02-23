(defproject cljaws "0.2.1-SNAPSHOT"
  :description "Clojure wrapper for Amazon Web Services"
  :dependencies [[org.clojure/clojure "1.1.0"]
                 [org.clojure/clojure-contrib "1.1.0"]
		 [net.java.dev.jets3t/jets3t "0.7.1"]
		 [com.google.code.typica/typica "1.6"]
		 ]
  :dev-dependencies [[leiningen/lein-swank "1.1.0"]
		     [lein-clojars "0.5.0-SNAPSHOT"]]

  :native-path "native" ; force forked jvm as a workaround
  )
