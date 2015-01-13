package jaglion;

import java.io.IOException;
import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class DEANONYMIZE extends EvalFunc<String>
{
    public Configuration config;
    public String rmap = "ReversMap";

    public String exec(Tuple input) throws IOException {

    // Check inputs to ensure there's only one value coming in and that it's not null
	if (input == null || input.size() != 1 || input.get(0) == null) 
	    return null;

	// Connect to the HBase database
	config = HBaseConfiguration.create();
	HBaseAdmin hba = new HBaseAdmin(config);
	HTableDescriptor tableDescriptor = null;
	HColumnDescriptor columnDescriptor = null;

	// Ensure the reverse table exists
	if (hba.tableExists(rmap) == false) {
	    throw new IOException("Value not found to map.");
	}

	try {
	    String code = (String)input.get(0);
	    String original = null;

	    // The backward map table
	    HTable backward = new HTable(config, rmap);

	    // Retrieve the value from the reverse map
	    Get g = new Get(Bytes.toBytes(code));
	    Result r = backward.get(g);
	    byte[] value = r.getValue(Bytes.toBytes("ActualValue"), Bytes.toBytes(""));
	    original = Bytes.toString(value);

	    return original;
	} catch(Exception e) {
	    System.out.println(input);
	    throw new IOException("Caught exception processing input row ", e);
	}
    }
}
