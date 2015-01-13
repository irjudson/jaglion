-- test.pig
REGISTER /usr/lib/zookeeper/zookeeper.jar;
REGISTER /usr/lib/hbase/hbase-client.jar;
REGISTER /usr/lib/hbase/hbase-common.jar;
REGISTER /usr/lib/hbase/hbase-protocol.jar;
REGISTER /usr/lib/hbase/hbase-hadoop-compat.jar;
REGISTER /usr/lib/hbase/lib/htrace-core.jar;

REGISTER bin/jaglion.jar;
A = LOAD 'testdata';
DUMP A;
B = FOREACH A GENERATE jaglion.ANONYMIZE($0, 0);
DUMP B;
-- C = FOREACH A GENERATE jaglion.ANONYMIZE($0, 1);
-- DUMP C;
-- D = FOREACH B GENERATE jaglion.DEANONYMIZE($0);
-- DUMP D;
-- E = FOREACH C GENERATE jaglion.DEANONYMIZE($0);
-- DUMP E;
