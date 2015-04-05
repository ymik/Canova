package org.canova.cli.subcommands;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestVectorize {

	@Test
	public void testLoadConfFile() {
		
		String confFile = "src/test/resources/csv/confs/unit_test_conf.txt";
		
		String[] foo = new String[1];
		foo[0] = "1";
		
		Vectorize vecCommand = new Vectorize( foo );
		
		vecCommand.configurationFile = confFile;
		vecCommand.loadConfigFile();
		
		// output.vector.format=svmlight
		assertEquals( "svmlight", vecCommand.configProps.get("output.vector.format") );
		
		// output.directory=/tmp/canova/cli/vectors/output/
		assertEquals( "/tmp/canova/cli/vectors/output/", vecCommand.configProps.get("output.directory") );
		
	}
	
	@Test
	public void testExecuteCSVConversionWorkflow() {
		
		String confFile = "src/test/resources/csv/confs/unit_test_conf.txt";
		
		String[] foo = new String[1];
		foo[0] = "1";
		
		Vectorize vecCommand = new Vectorize( foo );
		
		vecCommand.configurationFile = confFile;
		vecCommand.loadConfigFile();
		
		// output.vector.format=svmlight
		assertEquals( "svmlight", vecCommand.configProps.get("output.vector.format") );
		
		// output.directory=/tmp/canova/cli/vectors/output/
		assertEquals( "/tmp/canova/cli/vectors/output/", vecCommand.configProps.get("output.directory") );
		
		vecCommand.executeVectorizeWorkflow();
		
	}	

}
