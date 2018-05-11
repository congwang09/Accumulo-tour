package tour;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
// Classes you will use along the tour
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.TablePermission;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("Running the Accumulo tour. Having fun yet?");

        Path tempDir = Files.createTempDirectory(Paths.get("target"), "mac");
        MiniAccumuloCluster mac = new MiniAccumuloCluster(tempDir.toFile(), "tourguide");

        mac.start();
        exercise(mac);
        mac.stop();
    }

    static void exercise(MiniAccumuloCluster mac) throws Exception {
    	
    		
		// Connect to Mini Accumulo as the root user and create a table called "GothamPD".
		Connector conn = mac.getConnector("root", "tourguide");
		conn.tableOperations().create("GothamPD");
		
		// Create a "secretId" authorization & visibility
        final String secretId = "secretId";
        Authorizations auths = new Authorizations("secretId");
        ColumnVisibility colVis = new ColumnVisibility(secretId);
           
        
        // Create a user with the "secretId" authorization and grant him read permissions on our table
        conn.securityOperations().createLocalUser("commissioner", new PasswordToken("gordanrocks"));
        conn.securityOperations().changeUserAuthorizations("commissioner", auths);
        conn.securityOperations().grantTablePermission("commissioner", "GothamPD", TablePermission.READ);
		
		
		

		// Create a Mutation object to hold all changes to a row in a table.  Each row has a unique row ID.
		Mutation mutation = new Mutation("id0001");
		Value bruceWayne = new Value();
		byte[] bw = "Bruce Wayne".getBytes();
		Value bwn = new Value(bw);
		//byte[] bw2 = new byte[];
		
		// Create key/value pairs for Batman.  Put them in the "hero" family.
		mutation.put("hero","alias", colVis, "Batman");
		mutation.put("hero","name", colVis, "Bruce Wayne");
		mutation.put("hero","wearsCape?", colVis, "true");

		Mutation mutation2 = new Mutation("id0002");
		mutation2.put("hero","alias", "Robin");
		mutation2.put("hero","name", colVis, "Dick Grayson");
		mutation2.put("hero","wearsCape?", colVis, "true");
		
		Mutation mutation3 = new Mutation("id0003");
		mutation3.put("hero","alias", "Joker");
		mutation3.put("hero","name", "Unknown");
		mutation3.put("hero","wearsCape?", "false");

		
		
		// Create a BatchWriter to the GothamPD table and add your mutation to it. Try w/ resources will close for us.
		try (BatchWriter writer = conn.createBatchWriter("GothamPD", new BatchWriterConfig())) {
			writer.addMutation(mutation3);
			writer.addMutation(mutation2);
			writer.addMutation(mutation);
		}
		
		/*
		
		//Add serialized object
		String[] value = {"false", "writeToken", "scopeValue"};
		byte[] serializedByteArray = null;
		try {
			serializedByteArray = serialize(value);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		org.apache.accumulo.core.data.Value accumuloValue = new org.apache.accumulo.core.data.Value(serializedByteArray);
		Mutation mutation4 = new Mutation("id0002");
		mutation4.put("hero","alias2", accumuloValue);
		mutation4.put("hero","name2", colVis, "Dick Grayson2");
		mutation4.put("hero","wearsCape2?", colVis, "true2");
		
		
		try (BatchWriter writer = conn.createBatchWriter("GothamPD", new BatchWriterConfig())) {
			writer.addMutation(mutation4);
		}
		
		//Accumulo add HashSet with serialized object
		Map<String, Value> scopeOut = new HashMap<>();
		scopeOut = readOneRow( conn,  "GothamPD", new Text("id0002"), new Text("hero"), new Text("alias2"), "secretId");
		System.out.println(scopeOut.size());
	    for (Map.Entry<String, Value> entry : scopeOut.entrySet()) {
	    		Value arr = entry.getValue();
	    		String[] de = (String[]) deserialize(arr.get());
	    		System.out.println("ATTENTION!!!");
	    		for (String v : de)
	    			System.out.println(v);
	    		System.out.println("ATTENTION!!!");
	    }
		*/
		
		// Read and print all rows of the commissioner can see. Pass Scanner proper authorizations
		Connector commishConn = mac.getConnector("commissioner", "gordanrocks");
		
		
		//Authorizations authsTest = new Authorizations("secretId");
		//READ ONE ROW, working version!!!
/*		conn.securityOperations().changeUserAuthorizations("root", auths);
		Scanner scanner = conn.createScanner("GothamPD", auths);
		//scanner.setBatchSize(scanOpts.scanBatchSize);
		scanner.setRange(new Range("id0001"));
		scanner.fetchColumn(new Text("hero"), new Text("name"));
		for (Map.Entry<Key, Value> entry : scanner) {
	    		Key key = entry.getKey();
			Value value = entry.getValue();
	        	if (value == null) {
	        		System.out.println("value doesn't exist");
	        	}
	    		System.out.printf("Key : %-50s  Value : %s\n", key, value);
	    }*/
		
		
		JSONObject output = new JSONObject();
/*		//ScannerOpts scanOpts = new ScannerOpts();
		// Create a scanner
		conn.securityOperations().changeUserAuthorizations("root", auths);
	    Scanner scanner = conn.createScanner("GothamPD", new Authorizations("secretId"));
	    //scanner.setBatchSize(scanOpts.scanBatchSize);
	    // Say start key is the one with key of row
	    // and end key is the one that immediately follows the row
	    scanner.setRange(new Range("id0002"));
	    scanner.fetchColumn(new Text("hero"), new Text("alias2"));
	    Map<Key, Value> accEntries = new HashMap<>();
	    for (Map.Entry<Key, Value> entry : scanner) {
	    		Key key = entry.getKey();
			Value value = entry.getValue();
	        	try {
				output.put(key.toString(), value);
			} catch (JSONException e) {
				System.out.println("JSON Exception: " + e.getMessage());
			}
	    		System.out.printf("Key : %-50s  Value : %s\n", entry.getKey(), entry.getValue());
	    }
	    
	    
	    JSONArray arrJson=new JSONArray(output.toString());
	    for (int i = 0; i < output.toString().length(); i++) {
	    		System.out.println(arrJson.get(i));
	    }*/
		
		//Read one row using Hash Map
/*	    Map<String, Value> out = new HashMap<>();
	    out = readOneRow( conn,  "GothamPD", new Text("id0002"), new Text("hero"), new Text("alias2"), "secretId");
	    for (Map.Entry<String, Value> entry : out.entrySet()) {
	    		System.out.println(entry.getValue());
	    }*/
	    
	    /*int spacesToIndentEachLevel = 2;
	    System.out.println("here is the JSON!!!!!");
	    System.out.println(output.toString(spacesToIndentEachLevel));*/
	    
		
		//Delete one row!
		/*BatchDeleter deleter= conn.createBatchDeleter("GothamPD", auths, 1, new BatchWriterConfig());
		Collection<Range> ranges = new ArrayList<Range>();
		Scanner tableScannerRange= conn.createScanner("GothamPD", auths);
		
		tableScannerRange.setRange(Range.exact("id0001"));
		tableScannerRange.fetchColumn(new Text("hero"), new Text("name"));
		for (Entry<Key, Value> entry : tableScannerRange) {
            ranges.add(new Range(entry.getKey().getRow()));
		}
		deleter.setRanges(ranges);
		deleter.delete();*/
		JSONObject temp = new JSONObject();
		//modifyRow(Connector conn, String tableName, Text rowID, Text colFam, Text colQual, Value value, Text visibility) 
		temp = modifyRow(conn, "GothamPD", new Text("id0001"), new Text("hero"), new Text("name"), new Value("test"), new Text("secretId"));
		
		try (Scanner scan = commishConn.createScanner("GothamPD", auths)) {
			scan.setRange(new Range("id0001"));
			System.out.println("Gotham police department persons of interest: ");
			for (Map.Entry<Key, Value> entry : scan) {
				System.out.printf("Key: %-60s Value: %s\n", entry.getKey(), entry.getValue());
			}
		}
		
		//rowEntry = readOneRow(commishConn, "GothamPD", new Text("id0002"), secretId);

		
		
		
		
		// Read and print all rows of the "GothamPD" table. Try w/ resources will close for us.
		try (Scanner scan = conn.createScanner("GothamPD", Authorizations.EMPTY)) {
			System.out.println("Gotham Police Department Persons of Interest:");
			// A Scanner is an extension of java.lang.Iterable so behaves just like one.
			for (Map.Entry<Key, Value> entry : scan) {
				System.out.printf("Key : %-50s  Value : %s\n", entry.getKey(), entry.getValue());
			}
		}
	}
    
    public static JSONObject modifyRow(Connector conn, String tableName, Text rowID, Text colFam, Text colQual, Value value, Text visibility) {
		JSONObject output=new JSONObject();
		BatchWriter bw = null;
		Mutation mut1 = new Mutation(rowID);
		//Text colFam2 = new Text(colFam);
		//Text ColFam2ColQual1 = new Text(colQual);
		if(visibility!=null) {
			mut1.put(colFam, colQual, new ColumnVisibility(visibility), value);	
		} else {
			mut1.put(colFam, colQual, value);
		}
		try {
			bw = createBatchWriter(conn, tableName);
			bw.addMutation(mut1);
			bw.close(); // flushes and release ---no need for bw.flush()
		} catch (MutationsRejectedException e) {
			System.out.println("Failed mutation in updating  entry (" + rowID + ")");
			e.printStackTrace();
			try {
				return output.put("error", "Failed to modify scope");
			} catch (JSONException e1) {
				System.out.println("JSON Exception: " + e1.getMessage());
			}

		} catch (Exception e) {
			System.out.println("Failed mutation in updating  entry (" + rowID + ")");
			
		}

		try {
			output.put("contextID", rowID);
			output.put(colQual.toString(), value);
		} catch (JSONException e) {
			System.out.println("JSON Exception: " + e.getMessage());
		}		
		return output;
    }
    
    private static BatchWriter createBatchWriter(Connector conn, String table) {
		BatchWriter bw = null; 

		BatchWriterConfig bwConfig = new BatchWriterConfig();
		// bwConfig.setMaxMemory(memBuffer);
		bwConfig.setTimeout(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		// bwConfig.setMaxWriteThreads(numberOfThreads);
		try {
			bw = conn.createBatchWriter(table, bwConfig);
		} catch (TableNotFoundException e) {
			System.out.println("Unable to find table " + table
					+ " to create batchWriter.");

		}

		return bw;
    }
    
    public static byte[] serialize(Object obj) throws IOException {
	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    ObjectOutputStream os = new ObjectOutputStream(out);
	    os.writeObject(obj);
	    return out.toByteArray();
	}
	
	public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
	    ByteArrayInputStream in = new ByteArrayInputStream(data);
	    ObjectInputStream is = new ObjectInputStream(in);
	    return is.readObject();
	}
    
    
    public static Map<String, Value> readOneRow(Connector conn, String tableName, Text rowID, Text colFam, Text colQual, String visibility) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
		JSONObject output = new JSONObject();
		//ScannerOpts scanOpts = new ScannerOpts();
		// Create a scanner
		Authorizations auths = new Authorizations(visibility);
		conn.securityOperations().changeUserAuthorizations("root", auths);
	    Scanner scanner = conn.createScanner(tableName, auths);
	    //scanner.setBatchSize(scanOpts.scanBatchSize);
	    // Say start key is the one with key of row
	    // and end key is the one that immediately follows the row
	    scanner.setRange(new Range(rowID));
	    scanner.fetchColumn(colFam, colQual);
	    Map<String, Value> out = new HashMap<>();
	    for (Map.Entry<Key, Value> entry : scanner) {
	    		Key key = entry.getKey();
			Value value = entry.getValue();
	        
			out.put(key.toString(), value);
			
	    		System.out.printf("Key : %-50s  Value : %s\n", entry.getKey(), entry.getValue());
	    }
	    scanner.close();
	    return out;
	}
}
