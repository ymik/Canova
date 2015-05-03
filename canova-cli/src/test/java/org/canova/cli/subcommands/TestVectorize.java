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
		
		vecCommand.execute();
		
		// now check the output
		
	}	
	
	@Test
	public void testRandomShit() throws Exception {
		
		String[] args = { "-conf", "src/test/resources/csv/confs/unit_test_conf.txt" };		
		Vectorize vecCommand = new Vectorize( args );
		vecCommand.loadConfigFile();
		
		vecCommand.debugLoadedConfProperties();
		
		
		InputFormat in = vecCommand.createInputFormat();
		OutputFormat outputFormat = vecCommand.createOutputFormat();
		
		File tmp = new File("tmp.txt");
        FileUtils.writeLines(tmp, Arrays.asList("1","2","3"));
        InputSplit split = new FileSplit(tmp);
        tmp.deleteOnExit();
  //      RecordReader reader = new LineRecordReader();
   //     reader.initialize(split);
    
        RecordReader reader = in.createReader(split);
        
        //Collectionreader.next();
        
        File tmpOutSVMLightFile = new File("/tmp/vectorsTmp.svmLight");
        RecordWriter writer = new SVMLightRecordWriter(tmpOutSVMLightFile,true);
        
        
        int count = 0;
        while(reader.hasNext()) {
        	
        	Collection<Writable> w = reader.next();
        	System.out.println( "writeable: " + w.toArray()[0] );
        	
        	writer.write(w);
        	
            assertEquals(1,w.size());
            count++;
        } 
        
        
//        for(Collection<Writable> record : records)
 //           writer.write(record);

        writer.close();
     
		
		
	}

}
