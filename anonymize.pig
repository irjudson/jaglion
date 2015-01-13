-- anonymize.pig

-- -- Hadoop / HBase / Zookeeper 
REGISTER /usr/lib/zookeeper/zookeeper.jar;
REGISTER /usr/lib/hadoop/lib/commons-codec-1.4.jar;
REGISTER /usr/lib/hbase/hbase-client.jar;
REGISTER /usr/lib/hbase/hbase-common.jar;
REGISTER /usr/lib/hbase/hbase-protocol.jar;
REGISTER /usr/lib/hbase/hbase-hadoop-compat.jar;
REGISTER /usr/lib/hbase/lib/htrace-core.jar;

-- -- Our UDF
REGISTER bin/jaglion.jar;

-- Load the data with variable length records
-- pass $input to script as -param input='filename'
-- load each line as a character array
A = LOAD '$input' USING TextLoader AS (line:chararray);

-- Split each line into first, rest
SPLT = FOREACH A GENERATE STRSPLIT($0, ',', 2) AS A1;

-- For each row in the data, deanonymize the first column, use the rest as is
B = FOREACH SPLT GENERATE jaglion.ANONYMIZE(TRIM(A1.$0)), A1.$1;

-- Store the deanonymized results out to a file
-- pass $output to script as -param output='filename'
STORE B INTO '$output' using PigStorage(',');
