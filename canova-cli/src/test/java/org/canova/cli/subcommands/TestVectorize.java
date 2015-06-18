/*
 *
 *  *
 *  *  * Copyright 2015 Skymind,Inc.
 *  *  *
 *  *  *    Licensed under the Apache License, Version 2.0 (the "License");
 *  *  *    you may not use this file except in compliance with the License.
 *  *  *    You may obtain a copy of the License at
 *  *  *
 *  *  *        http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  *    Unless required by applicable law or agreed to in writing, software
 *  *  *    distributed under the License is distributed on an "AS IS" BASIS,
 *  *  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  *    See the License for the specific language governing permissions and
 *  *  *    limitations under the License.
 *  *
 *
 */

package org.canova.cli.subcommands;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URL;
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
import org.canova.api.util.ArchiveUtils;
import org.canova.api.writable.Writable;
import org.canova.image.mnist.MnistFetcher;
import org.canova.image.recordreader.MNISTRecordReader;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

public class TestVectorize {

	private static final String trainingFilesFilename = "images-idx1-ubyte.gz";
	private static final String trainingFilesURL = "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz";
	public static final String trainingFilesFilename_unzipped = "images-idx1-ubyte";

	private static final String trainingFileLabelsURL = "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz";
	private static final String trainingFileLabelsFilename = "labels-idx1-ubyte.gz";
	public static final String trainingFileLabelsFilename_unzipped = "labels-idx1-ubyte";
	
	
    static String TEMP_ROOT = "/tmp"; //System.getProperty("user.home");
    static String MNIST_ROOT = TEMP_ROOT + File.separator + "MNIST" + File.separator;   
    
    static String MNIST_Filename = MNIST_ROOT + MNISTRecordReader.trainingFilesFilename_unzipped;
	
	
	public void checkForMNISTLocally() {
		
    	
    	// 1. check for the MNIST data first!
    	
    	// does it exist?
    	
    	// if not, then let's download it
    	
        System.out.println( "Checking to see if MNIST exists locally: " + MNIST_ROOT );
        
        if(!new File(MNIST_ROOT).exists()) {
        	
        	new File(MNIST_ROOT).mkdir();
        	
        	System.out.println("Downloading and unzipping the MNIST dataset locally to: " + MNIST_ROOT );
            try {
				//new MnistFetcher().downloadAndUntar();
            	downloadAndUntar();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        } else {
        	
        	System.out.println( "MNIST already exists locally..." );
        	
        }
        
        if ( new File(MNIST_Filename).exists() ) {
        	System.out.println( "The images file exists locally unzipped!" );
        } else {
        	System.out.println( "The images file DOES NOT exist locally unzipped!" );
        }		
		
	}
	
	
	

	/**
	 * Added another copy of this method because MNISTFetcher uses the user's system home directory
	 * and we couldnt specify that in the canova conf file
	 * 
	 * @return
	 * @throws IOException
	 */
	public static File downloadAndUntar() throws IOException {
		
		File fileDir;
		
		
//		if(fileDir != null) {
	//		return fileDir;
		//}
		// mac gives unique tmp each run and we want to store this persist
		// this data across restarts
		//File tmpDir = new File(System.getProperty("user.home"));

		File baseDir = new File( MNIST_ROOT );
		if(!(baseDir.isDirectory() || baseDir.mkdir())) {
			throw new IOException("Could not mkdir " + baseDir);
		}


		//log.info("Downloading mnist...");
		// getFromOrigin training records
		File tarFile = new File(baseDir, trainingFilesFilename);

		if(!tarFile.isFile()) {
			FileUtils.copyURLToFile(new URL(trainingFilesURL), tarFile);
		}

	    ArchiveUtils.unzipFileTo(tarFile.getAbsolutePath(),baseDir.getAbsolutePath());




        // getFromOrigin training records
        File labels = new File(baseDir, trainingFileLabelsFilename);

        if(!labels.isFile()) {
            FileUtils.copyURLToFile(new URL(trainingFileLabelsURL), labels);
        }

        ArchiveUtils.unzipFileTo(labels.getAbsolutePath(), baseDir.getAbsolutePath());



        fileDir = baseDir;
		return fileDir;
	}	
	
	public static File download_LFW_AndUntar(String workingBaseDir) throws IOException {
		
		String lfwURL = "http://vis-www.cs.umass.edu/lfw/lfw-bush.tgz";
		String lfwUrlFilename = "lfw-bush.tgz";
		File fileDir;
		
		
//		if(fileDir != null) {
	//		return fileDir;
		//}
		// mac gives unique tmp each run and we want to store this persist
		// this data across restarts
		//File tmpDir = new File(System.getProperty("user.home"));

		File baseDir = new File( workingBaseDir );
		if(!(baseDir.isDirectory() || baseDir.mkdir())) {
			throw new IOException("Could not mkdir " + baseDir);
		}


		//log.info("Downloading mnist...");
		// getFromOrigin training records
		File tarFile = new File(baseDir, lfwUrlFilename);

		if(!tarFile.isFile()) {
			FileUtils.copyURLToFile(new URL( lfwURL ), tarFile);
		}

	    ArchiveUtils.unzipFileTo(tarFile.getAbsolutePath(),baseDir.getAbsolutePath());


        fileDir = baseDir;
		return fileDir;
	}		
	
	
	public static void setupLFWSampleLocally() throws IOException {
		
		
		String localUnzippedSubdir = "lfw";
		String workingDir = "/tmp/canova/image/"; // + localUnzippedSubdir; 
		
		// does the file exist locally?
		
		download_LFW_AndUntar( workingDir );
		
		// let's only get a few images in 2 labels
		
		
	}
	
	@Test
	public void testLoadConfFile() throws IOException {
				
		String[] args = { "-conf", "src/test/resources/csv/confs/unit_test_conf.txt" };		
		Vectorize vecCommand = new Vectorize( args );
		
		vecCommand.loadConfigFile();
		assertEquals( "/tmp/iris_unit_test_sample.txt", vecCommand.configProps.get("output.directory") );
		
	}
	
	@Test
	public void testExecuteCSVConversionWorkflow() throws Exception {
		
		String[] args = { "-conf", "src/test/resources/csv/confs/unit_test_conf.txt" };		
		Vectorize vecCommand = new Vectorize( args );
		
		vecCommand.execute();
		
		// now check the output
		
	}	
	

	
	@Test
	public void testExecuteImageCustomMNISTInputFormatConversionWorkflow() throws Exception {
		
		checkForMNISTLocally();
		
		String[] args = { "-conf", "src/test/resources/csv/confs/image/mnist/unit_test_conf.txt" };		
		Vectorize vecCommand = new Vectorize( args );
		
		vecCommand.execute();
		
		// now check the output
		
	}	
	
	/**
	 * TODO: need some work here on the LFW test data
	 * 
	 * 
	 * @throws Exception
	 */
	@Test
	public void testExecuteImageInputFormatConversionWorkflow() throws Exception {
		
//	1. setup the LWF dataset sample to work with
		
		setupLFWSampleLocally();
		
		String[] args = { "-conf", "src/test/resources/image/conf/unit_test_conf.txt" };		
		Vectorize vecCommand = new Vectorize( args );
		
		vecCommand.execute();
		
		// now check the output
		
		// 1. how many labels are there?
		
		// 2. how many vectors are in the output?
		
	}	
	
	/**
	 * 
	 * TODO: this needs to work for chapter 4
	 * 
	 * 1. how do we label docs? (directories)
	 * 
	 * 
	 * 2. do we only support local mode for Text vectorization?
	 * 		-	at this point the current Text Vectorization setup has to be refactored to 
	 * 			run in a parallel fashion
	 * 
	 * 
	 * @throws Exception
	 */
	@Test
	public void testExecuteTextInputFormat_TFIDF_ConversionWorkflow() throws Exception {
		
		String[] args = { "-conf", "src/test/resources/text/conf/text_vectorization_conf_unit_test.txt" };		
		Vectorize vecCommand = new Vectorize( args );
		
		vecCommand.execute();
	/*	
		InputSplit split = new FileSplit(new ClassPathResource("text/data/unit_test_0/").getFile());
		
        File inputFile = new File( "src/test/resources/text/data/unit_test_0/" );
        InputSplit splitAlt = new FileSplit(inputFile);
        //InputFormat inputFormat = this.createInputFormat();
		
		System.out.println( "split1: " + split.locations()[0].getPath() );
		System.out.println( "split2: " + splitAlt.locations()[0].getPath() );
		*/
		
		// now check the output
		
	}		

	
	
}
