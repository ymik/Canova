package org.canova.cli.driver;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestCommandLineInterfaceDriver {

	@Test
	public void testMainCLIDriverEntryPoint() {
		
		String[] args = { "vectorize", "-conf", "src/test/resources/csv/confs/unit_test_conf.txt" };
		
		CommandLineInterfaceDriver.main( args );
		
	}

}
