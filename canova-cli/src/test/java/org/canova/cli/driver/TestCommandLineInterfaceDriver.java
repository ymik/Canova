package org.canova.cli.driver;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TestCommandLineInterfaceDriver {

	@Test
	public void testMainCLIDriverEntryPoint() throws IOException {
		
		String[] args = { "vectorize", "-conf", "src/test/resources/csv/confs/unit_test_conf.txt" };
		
		CommandLineInterfaceDriver.main( args );
		
		String outputFile = "/tmp/iris_unit_test_sample.txt";
		
		ArrayList<String> vectors = new ArrayList<String>();
		
		Map<String, Integer> labels = new HashMap<String, Integer>();
		
		try (BufferedReader br = new BufferedReader(new FileReader( outputFile ))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		       // process the line.
		    	if ("".equals(line.trim())) {
		    		
		    	} else {
		    		vectors.add( line );
		    		
		    		String parts[] = line.split(" ");
		    		String key = parts[0];
		    		if (labels.containsKey(key)) {
		    			Integer count = labels.get(key);
		    			count++;
		    			labels.put(key, count);
		    		} else {
		    			labels.put(key, 1);
		    		}
		    		
		    	}
		    }
		}
		
		assertEquals( 12, vectors.size() );
		assertEquals( 3, labels.size() );
		
		
	}

}
