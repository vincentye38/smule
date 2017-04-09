The dataset is attached (incidences_piano.tsv):  Three columns corresponding to user_id (payer), item_id (music), timestamp.  You don't need to use them all problem:

How-to-build:
mvn clean package

Mavin will build two jars under target/ directory. target/smule-1.0-jar-with-dependencies.jar packs all the dependencies as a uber jar. target/smule-1.0.jar doesn't have dependencies packad. It can be submitted to spark cluster using sparksubmit command.

(1) Write your own functions and classes in Scala (stand alone or over Spark) to calculate item-item similarity score (e.g., cosine ).  Package the codes to show your production ability.  Note, you may not pay attention to use all the data, dealing with data quality issues, or correctness of the answer. In other words, using a subset of the data to demo your ability to coding and deploying.
standalone application:
java -cp smule-1.0-jar-with-dependencies.jar com.smule.ML.simularity --master local --appName simLocal /temp/incidences_piano /temp/incidences_piano_sim

(2) Code Spark solution with MLlib to do matrix factorization.
standalone application:
java -cp target/smule-1.0-jar-with-dependencies.jar com.smule.ML.Factorization --master local --appName simLocal /Users/vincenty/Downloads/incidences_piano.tsv /Users/vincenty/Downloads/incidences_piano_fac /Users/vincenty/Downloads/dictionary
