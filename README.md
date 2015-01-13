jaglion
=======

Tools for doing hybrid cloud hadoop jobs with Azure and Cloudera.

How to build the User Defined Function (UDF) Jar

# Compile
javac -classpath "\`hbase classpath\`/usr/lib/pig/pig.jar:/usr/lib/hadoop/lib/commons-code-1.4.jar" ANONYMIZE.java DEANONYMIZE.java

# Jar
mkdir jaglion
mv ANONYMIZE.class DEANONYMIZE.class jaglion
jar -cf ../bin/jaglion.jar jaglion

# Execute local test
java -classpath "\`hbase classpath\`/usr/lib/pig/pig.jar:/usr/lib/hadoop/lib/commons-code-1.4.jar" org.apache.pig.Main -x local test.pig
