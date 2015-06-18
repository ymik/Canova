package org.canova.cli.vectorization;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.canova.api.formats.input.InputFormat;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.records.writer.RecordWriter;
import org.canova.api.records.writer.impl.SVMLightRecordWriter;
import org.canova.api.split.FileSplit;
import org.canova.api.split.InputSplit;
import org.canova.api.split.InputStreamInputSplit;
import org.canova.image.mnist.MnistFetcher;
import org.canova.image.recordreader.ImageRecordReader;
import org.canova.image.recordreader.MNISTRecordReader;

import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

public class TestImageVectorizationEngine {

	
	
	@Test
	public void testInputFormatWithImageEngine() throws IOException, InterruptedException {

        String TEMP_ROOT = System.getProperty("user.home");
        String MNIST_ROOT = TEMP_ROOT + File.separator + "MNIST" + File.separator;   
        
        String MNIST_Filename = MNIST_ROOT + MNISTRecordReader.trainingFilesFilename_unzipped;
    	
    	// 1. check for the MNIST data first!
    	
    	// does it exist?
    	
    	// if not, then let's download it
    	
        System.out.println( "Checking to see if MNIST exists locally: " + MNIST_ROOT );
        
        if(!new File(MNIST_ROOT).exists()) {
        	System.out.println("Downloading and unzipping the MNIST dataset locally to: " + MNIST_ROOT );
            new MnistFetcher().downloadAndUntar();
        } else {
        	
        	System.out.println( "MNIST already exists locally..." );
        	
        }
        
        if ( new File(MNIST_Filename).exists() ) {
        	System.out.println( "The images file exists locally unzipped!" );
        } else {
        	System.out.println( "The images file DOES NOT exist locally unzipped!" );
        }		
		
		/*
        RecordReader reader = new ImageRecordReader(28,28,false);
        ClassPathResource res = new ClassPathResource("/test.jpg");
        reader.initialize(new InputStreamInputSplit(res.getInputStream(), res.getURI()));
        assertTrue(reader.hasNext());
		*/
        
        RecordReader reader = new MNISTRecordReader();
        
        //ClassPathResource res = new ClassPathResource( MNIST_Filename );
        File resMNIST = new File( MNIST_Filename );
        InputStream targetStream = new FileInputStream( resMNIST );
       // resMNIST.
        
        reader.initialize(new InputStreamInputSplit( targetStream, resMNIST.toURI() ) );
        
        assertTrue(reader.hasNext());
        
        
        File out = new File("/tmp/mnist_svmLight_out.txt");
        //out.deleteOnExit();
        RecordWriter writer = new SVMLightRecordWriter(out,true);
		
		ImageVectorizationEngine engine = new ImageVectorizationEngine();
//		engine.initialize( new InputStreamInputSplit( targetStream, resMNIST.toURI() ), reader, writer);
		
		// setup split and reader
		

        String datasetInputPath = "";
        /*
        File inputFile = new File(datasetInputPath);
        InputSplit split = new FileSplit(inputFile);
        InputFormat inputFormat = this.createInputFormat();

        RecordReader reader = inputFormat.createReader(split);
*/
        		
        
        engine.execute();

        // check out many records are in the output
		
	}

}
