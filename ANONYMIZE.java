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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class ANONYMIZE extends EvalFunc<String>
{
    public Configuration config;
    public String fmap = "FowardMap";
    public String rmap = "ReversMap";

    public String exec(Tuple input) throws IOException {

    // As long as we have a reasonable inputs
	if (input.size() != 2 || input.get(0) == null || input.get(1) == null) 
	    return null;

	// Connect to HBASE
	config = HBaseConfiguration.create();
	HBaseAdmin hba = new HBaseAdmin(config);
	HTableDescriptor tableDescriptor = null;
	HColumnDescriptor columnDescriptor = null;

	// Check and optionally create the forward mapping table
	if (hba.tableExists(fmap) == false) {
	    tableDescriptor = new HTableDescriptor(fmap);
	    columnDescriptor = new HColumnDescriptor("AnonymousValue");
	    tableDescriptor.addFamily(columnDescriptor);
	    hba.createTable(tableDescriptor);
	}

	// Check and optionally create the reverse map table
	if (hba.tableExists(rmap) == false) {
	    tableDescriptor = new HTableDescriptor(rmap);
	    columnDescriptor = new HColumnDescriptor("ActualValue");
	    tableDescriptor.addFamily(columnDescriptor);
	    hba.createTable(tableDescriptor);
	}

	try {
		// Read inputs
	    String id = (String)input.get(0);
	    int unique = (int)input.get(1);
	    System.out.println("Value: " + id + " Unique: " + unique);
	    Put p = null;

	    // Generate value for input id
	    String uuid = UUID.randomUUID().toString();
	    String hash = DigestUtils.shaHex(id);
	    
	    System.out.println("UUID: " + uuid + " Hash: " + hash);

	    // Store the value forward and backward
	    HTable forward = new HTable(config, fmap);
	    HTable backward = new HTable(config, rmap);

	    // Forward Map
	    p = new Put(Bytes.toBytes(id));
	    p.add(Bytes.toBytes("AnonymousValue"), Bytes.toBytes("hash"), Bytes.toBytes(hash));
	    p.add(Bytes.toBytes("AnonymousValue"), Bytes.toBytes("uuid"), Bytes.toBytes(uuid));
	    forward.put(p);

	    System.out.println("Stored forward map.");

	    // Reverse Map - Hash
	    p = new Put(Bytes.toBytes(hash));
	    p.add(Bytes.toBytes("ActualValue"), Bytes.toBytes(""), Bytes.toBytes(id));
	    backward.put(p);

	    System.out.println("Stored reverse map - hash.");

	    // Reverse Map - UUID
	    p = new Put(Bytes.toBytes(uuid));
	    p.add(Bytes.toBytes("ActualValue"), Bytes.toBytes(""), Bytes.toBytes(id));
	    backward.put(p);

	    System.out.println("Stored reverse map - uuid.");
	    
	    // If the request was for a unique value return the uuid
	    if (unique == 1) {
			return uuid;
	    } else {
	    	// otherwise return the hashed value
			return hash;
	    }
	} catch(Exception e) {
	    System.out.println(input);
	    throw new IOException("Caught exception processing input row ", e);
	}
    }
}
