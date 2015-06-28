package org.canova.image.recordreader;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.InputStreamInputSplit;
import org.canova.api.writable.Writable;
import org.canova.image.mnist.MnistFetcher;
import org.canova.image.mnist.MnistManager;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

public class TestMNISTRecordReader {

	
    @Test
    public void testMNISTRecordReadeR_InputStream() throws Exception {
    	
    	//private transient MnistManager man;
        final int NUM_EXAMPLES = 60000;
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
        try {
            //man = new MnistManager(MNIST_ROOT + MnistFetcher.trainingFilesFilename_unzipped, MNIST_ROOT + MnistFetcher.trainingFileLabelsFilename_unzipped);
        }catch(Exception e) {
            FileUtils.deleteDirectory(new File(MNIST_ROOT));
            new MnistFetcher().downloadAndUntar();
            //man = new MnistManager(MNIST_ROOT + MnistFetcher.trainingFilesFilename_unzipped, MNIST_ROOT + MnistFetcher.trainingFileLabelsFilename_unzipped);

        }        
        */
    	// next, lets fire up the record reader and give it a whirl...
    	
        RecordReader reader = new MNISTRecordReader();
        
        //ClassPathResource res = new ClassPathResource( MNIST_Filename );
        File resMNIST = new File( MNIST_Filename );
        InputStream targetStream = new FileInputStream( resMNIST );
       // resMNIST.
        
        reader.initialize(new InputStreamInputSplit( targetStream, resMNIST.toURI() ) );
        
        assertTrue(reader.hasNext());
        
        for ( int x = 0; x < 20; x++ ) {
        	
        	if ( reader.hasNext() ) {
        		
        	
	        	Collection<Writable> record = reader.next();
	        
	        	// 784 pixels converted to vector entries + a class label entry == 785
	        	assertEquals( 785, record.size() );
	        
	       // 	if ( x % 1000 == 0 ) {
	       // 		System.out.println( "Image " + x  );
	       // 		System.out.println( "mid Entry: " + record.toArray()[492] );
	       // 		System.out.println( "Label Entry: " + record.toArray()[784] );
	       // 	}
	        	
        	} else {
        		
        		System.out.println( "Reader returned end of images at counter: " + x );
        	}
        	
        }
        
        System.out.println(  "looped through 60k examples" );
        
    }
	

}
