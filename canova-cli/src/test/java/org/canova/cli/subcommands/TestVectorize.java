package org.canova.cli.subcommands;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.canova.api.exceptions.CanovaException;
import org.canova.api.formats.input.InputFormat;
import org.canova.api.formats.output.OutputFormat;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.records.reader.impl.LineRecordReader;
import org.canova.api.records.writer.RecordWriter;
import org.canova.api.records.writer.impl.SVMLightRecordWriter;
import org.canova.api.split.FileSplit;
import org.canova.api.split.InputSplit;
import org.canova.api.writable.Writable;
import org.junit.Test;

public class TestVectorize {

	@Test
	public void testLoadConfFile() throws IOException {
				
		String[] args = { "-conf", "src/test/resources/csv/confs/unit_test_conf.txt" };		
		Vectorize vecCommand = new Vectorize( args );
		
		vecCommand.loadConfigFile();
		assertEquals( "/tmp/iris_unit_test_sample.txt", vecCommand.configProps.get("output.directory") );
		
	}
	
	@Test
	public void testExecuteCSVConversionWorkflow() throws CanovaException, IOException, InterruptedException {
		
		String[] args = { "-conf", "src/test/resources/csv/confs/unit_test_conf.txt" };		
		Vectorize vecCommand = new Vectorize( args );
		
		vecCommand.executeVectorizeWorkflow();
		
		// now check the output
		
	}	
	


}
