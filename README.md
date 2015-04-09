Data Anonymizing in Hadoop using Pig
=======
Jaglion provides data anonymizing capabilities as Pig UDFs. They are extremely useful if PII data from an on premise Hadoop cluster needs to be moved to a cloud based service for processing (e.g. machine learning, Monte Carlo modelling, ...). One possible approach would be to simply remove all PII related data from the dataset. However this is not feasible because in the case of machine learning, the predictive models and its predictions are related to individual customers. 

This allows us to embed anonymizing and de-anonymizing as part of an on premise Pig data transformation job, ensuring no PII goes off-premise:
Here an example anonymizing data (in this case the ownerId) as part of a Pig script:

    A = LOAD 'data/xyz_device' using PigStorage(';') AS (
    	ownerId: chararray, 
    	specNr: chararray,
    	senderId: chararray,
    	deviceData: chararray);
    B = FOREACH A GENERATE jaglion.ANONYMIZE(ownerId), specNr, senderId, deviceData;
    jaglion.WASBSTORE B INTO 'data/devices/anonym/xyz_device' USING PigStorage (';');

And here an example de-anonymizing data:

    A = jaglion.WASBLOAD 'data/results/anonym/xyz_result' using PigStorage(';') AS (
    	ownerId: chararray, 
    	resultData: chararray);
    B = FOREACH A GENERATE jaglion.DEANONYMIZE(ownerId), resultData;
    STORE B INTO 'data/xyz_result' USING PigStorage (';');

The anonymizing function uses Hadoop’s HBASE as the persistent key/value store to retrieve and store the PII data correlation. This HBASE instance is part of the on premise Hadoop cluster.

There are two different correlation modes for different level of privacy:

**High privacy mode**

Each time the ANONYMIZE function is called, it returns a unique id. In this mode, even if the customerId is the same, the function will return two different anonymized ids. This mode ensures that off premise data can’t be correlated across the anonymized dimension.

    ANONYMIZE(customerId) != ANONYMIZE(customerId)

High privacy mode can be used for independent jobs such as pattern recognition, large scale data transformation or model calculations. However it won’t be useful for jobs which gain insight by correlating multiple dependent data items (such as machine learning).

**Medium privacy mode**

The ANONYMIZE function returns always the same anonymized id for the same request. This mode allows for off premise correlations across the same anonymized ids. 

	ANONYMIZE(customerId) == ANONYMIZE(customerId)

##How to build the User Defined Function (UDF) Jar

**Compile**

    `javac -classpath "\`hbase classpath\`/usr/lib/pig/pig.jar:/usr/lib/hadoop/lib/commons-code-1.4.jar" ANONYMIZE.java DEANONYMIZE.java`

**Jar**

    mkdir jaglion
    mv ANONYMIZE.class DEANONYMIZE.class jaglion
    jar -cf ../bin/jaglion.jar jaglion
    
##How to use the UDFs in Pig
To use the UDFs, the java package and its dependencies needs to be registered first:

	REGISTER /usr/lib/zookeeper/zookeeper.jar;
	REGISTER /usr/lib/hbase/hbase-client.jar;
	REGISTER /usr/lib/hbase/hbase-common.jar;
	REGISTER /usr/lib/hbase/hbase-protocol.jar;
	REGISTER /usr/lib/hbase/hbase-hadoop-compat.jar;
	REGISTER /usr/lib/hbase/lib/htrace-core.jar;

	REGISTER bin/jaglion.jar;

Now, two UDFs can be used within Pig. Assume we loaded the data into A using a statement similar to 

	A = LOAD 'testdata';
Once A is loaded, the statement below anonymizes the first column in A using medium privacy (the same value will always generate the same anonymized value):

	B = FOREACH A GENERATE jaglion.ANONYMIZE($0, 0);

The following statement will generate a unique anonymized value for each value in A, regardless if the source values are the same:

	C = FOREACH A GENERATE jaglion.ANONYMIZE($0, 1);
To de-anonymize, we simply call the DEANONYMIZE function:

	D = FOREACH B GENERATE jaglion.DEANONYMIZE($0);
	E = FOREACH C GENERATE jaglion.DEANONYMIZE($0);

##Execute local test
1. Copy the file "testdata" to HDFS
2. Copy "jaglion.jar" into the directory which is declared when registering the UDFs in Pig (e.g. bin)
3. Run the local test (specifying the "hbase classpath")

	    java -classpath "\`hbase classpath\`/usr/lib/pig/pig.jar:/usr/lib/hadoop/lib/commons-code-1.4.jar" org.apache.pig.Main -x local test.pig
