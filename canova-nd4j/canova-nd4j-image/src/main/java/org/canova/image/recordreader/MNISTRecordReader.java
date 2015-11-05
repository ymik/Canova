package org.canova.image.recordreader;

import org.canova.api.split.FileSplit;
import org.canova.api.split.InputSplit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.canova.api.conf.Configuration;

import org.canova.api.records.reader.RecordReader;

import org.nd4j.linalg.factory.Nd4j;

import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;
import org.canova.image.mnist.MnistManager;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.util.ArrayUtil;
import org.nd4j.linalg.util.FeatureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * Record reader that understands the MNIST file format as described here:
 * 
 * http://yann.lecun.com/exdb/mnist/
 * 
 * Not built to handle splits of the file, for now forces a single worker to process file
 * 
 * Why?
 * - the MNIST training file is 47MB unzipped
 * - right now (June 2015) Canova only runs in local/serial mode
 * - when we add MapReduce as a runtime engine, the training file size (47MB) is still smaller 
 * 		than the lowest production block size in HDFS these days (64MB, 128MB), so normally MapReduce's
 * 		scheduling system would not split the file (unless you manually set the file's block size lower)
 * - This input format's main purpose is to read MNIST raw data into Canova to be written out
 * 		as another format (SVMLight most likely) for DL4J's input format's to read  
 *
 * 
 * 
 * Assumes that file exists locally and has been unzipped
 * 
 * Why?
 * -	When we do port this input format to be HDFS-aware, these mechanics will be incompatible
 * 			(we dont want N workers all trying to download files or coordinate who is downloading the file)
 * 
 * @author Josh Patterson
 *
 */
public class MNISTRecordReader implements RecordReader {

	private static Logger log = LoggerFactory.getLogger(MNISTRecordReader.class);

	private URI[] locations;
    private int currIndex = 0;
    private Iterator<String> iter;	
    
    private transient MnistManager man;

    // is this only valid for the training data?
    public final static int NUM_EXAMPLES = 60000;
    private int numOutcomes = 0;
    private int totalExamples = 0;
    private int cursor = 0;
    private int inputColumns = 0;
    
    protected DataSet curr = null;
    
    private File fileDir;

    private static final String trainingFilesURL = "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz";

	private static final String trainingFilesFilename = "images-idx1-ubyte.gz";
	public static final String trainingFilesFilename_unzipped = "images-idx1-ubyte";

	private static final String trainingFileLabelsURL = "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz";
	private static final String trainingFileLabelsFilename = "labels-idx1-ubyte.gz";
	public static final String trainingFileLabelsFilename_unzipped = "labels-idx1-ubyte";
	private static final String LOCAL_DIR_NAME = "MNIST";    
    
    // this is only valid when we're running in local mode
    private static final String TEMP_ROOT = System.getProperty("user.home");
    private static final String MNIST_ROOT = TEMP_ROOT + File.separator + "MNIST" + File.separator;
    
    // for now we always want to binarize
    private boolean binarize = true;    
	
    public MNISTRecordReader() throws IOException {
    	
		this.man = new MnistManager(MNIST_ROOT + trainingFilesFilename_unzipped, MNIST_ROOT + trainingFileLabelsFilename_unzipped);
    	
    	this.numOutcomes = 10;
        this.binarize = binarize;
        this.totalExamples = NUM_EXAMPLES;
        //1 based cursor
        this.cursor = 1;
        man.setCurrent(cursor);
        int[][] image;
        try {
            image = man.readImage();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read image");
        }
        inputColumns = ArrayUtil.flatten(image).length;    	
    	
    }
    

    @Override
    public void initialize(InputSplit split) throws IOException, InterruptedException {

    	this.locations = split.locations();
        if (locations != null && locations.length > 0) {
            iter = IOUtils.lineIterator(new InputStreamReader(locations[0].toURL().openStream()));
        }
    	

    }

    @Override
    public void initialize(Configuration conf, InputSplit split) throws IOException, InterruptedException {
        initialize(split);
    }

    @Override
    public void initialize(String basePath, int seed, int numExamples) throws IOException {

    }

    /**
     * Basic logic:
     * 
     * 		1. hit fetch() and have the MNISTManager class go pull a batch from the file (converting them over to matrices)
     * 		2. loop through the DataSet(s) and convert them into a list of Writables
     * 
     * 
     * Criticism of current design
     * -	makes two passes over result set for conversion
     * 
     * 
     * Reasoning for current design
     * -	want to re-use older existing code, and need to get this out the door
     * -	dataset is bounded to 60k records at most thus input is always constant 
     * -	this input format never gets used beyond converting a demo dataset that's bounded
     * 
     * 
     * 
     *  LABEL
     *  	-	added as last value
     * 
     */
    @Override
    public Collection<Writable> next() {	

    	// we only want one image record:label pair at a time
    	if (this.fetchNext() == false) {
    		return null;
    	}
    	
    	DataSet currentRecord = this.curr;
    	
        List<Writable> ret = new ArrayList<>();
        //try {
        	
        
        INDArray data = currentRecord.get(0).getFeatureMatrix();
        INDArray labels = currentRecord.get(0).getLabels();
        
        	// get the record row
       //     INDArray row = imageLoader.asRowVector(image);
            
        //System.out.println( "size of label vector: " + labels.length() );
        
            // convert it to a List<Writable>
            for (int i = 0; i < data.length(); i++) {
                ret.add( new DoubleWritable( data.getDouble( i ) ) );
            }
          
        // get the label    
        //    if(appendLabel)
        //        ret.add(new DoubleWritable(labels.indexOf(image.getParentFile().getName())));

            for (int i = 0; i < labels.length(); i++) {
            	if (labels.getDouble( i ) > 0 ) {
            		//System.out.println( "adding label: " + i + " - " + labels.getDouble( i ) );
            		ret.add( new DoubleWritable( i ) );
            		break; 
            	}
            }

            
            //ret.add( new DoubleWritable( labels.getDouble(0) ) );
    
            

//        throw new IllegalStateException("No more elements");
    	
  
       return ret;
    	
//    	return null;
    }
    

    @Override
    public boolean hasNext() {
    //    return iter != null && iter.hasNext();
    	if ( cursor < totalExamples ) {
    		return true;
    	}
    	return false;
    }

    @Override
    public void close() {

    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }    
    
    
    
    
    
    /**
     * Based on logic from fetcher:
     * 
     * https://github.com/deeplearning4j/deeplearning4j/blob/master/deeplearning4j-core/src/main/java/org/deeplearning4j/datasets/fetchers/MnistDataFetcher.java
     * 
     * 
     * The cursor logic here is broken.
     * 	-	we need to make sure we can get one example and advance the cursor
     * 
     * @param numExamples
     */
    public boolean fetchNext() { //int numExamples) {
    	
        //if(!hasMore()) {
    	if ( !this.hasNext() ) {
            return false; // new IllegalStateException("Unable to getFromOrigin more; there are no more images");
        }



        //we need to ensure that we don't overshoot the number of examples total
        List<DataSet> toConvert = new ArrayList<>();

//        for (int i = 0; i < numExamples; i++,cursor++) {
            
        	/*
            if (man == null) {
                try {
                    man = new MnistManager(MNIST_ROOT + MnistFetcher.trainingFilesFilename_unzipped,MNIST_ROOT + MnistFetcher.trainingFileLabelsFilename_unzipped);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            */
        	
        	// tell MNISTManager where we want to be
            man.setCurrent(cursor);
            
            // pull the image and do some baseline transform
            //note data normalization
            try {
                INDArray in = ArrayUtil.toNDArray(ArrayUtil.flatten(man.readImage()));
                if(binarize) {
                    for(int d = 0; d < in.length(); d++) {
                        if(binarize) {
                            if(in.getDouble(d) > 30) {
                                in.putScalar(d,1);
                            }
                            else {
                                in.putScalar(d,0);
                            }

                        }


                    }
                }
                else {
                    in.divi(255);
                }


                INDArray out = createOutputVector(man.readLabel());
                boolean found = false;
                for(int col = 0; col < out.length(); col++) {
                    if(out.getDouble(col) > 0) {
                        found = true;
                        break;
                    }
                }
                if(!found) {
                    throw new IllegalStateException("Found a matrix without an outcome");
                }

                toConvert.add(new DataSet(in,out));
            } catch (IOException e) {
                throw new IllegalStateException("Unable to read image");

            }
            
            // update our cursor
            this.cursor++;
            
        //}


        initializeCurrFromList(toConvert);


        return true;

    }
    
    /**
	 * Creates an output label matrix
	 * @param outcomeLabel the outcome label to use
	 * @return a binary vector where 1 is applyTransformToDestination to the
	 * index specified by outcomeLabel
	 */
	protected INDArray createOutputVector(int outcomeLabel) {
		return FeatureUtil.toOutcomeVector(outcomeLabel, numOutcomes);
	}  
	
	/**
	 * Creates a feature vector
	 * @param numRows the number of examples
 	 * @return a feature vector
	 */
	protected INDArray createInputMatrix(int numRows) {
		return Nd4j.create(numRows, inputColumns);
	}
	
	
	protected INDArray createOutputMatrix(int numRows) {
		return Nd4j.create(numRows,numOutcomes);
	}	
    

	/**
	 * Initializes this data transform fetcher from the passed in datasets
	 * @param examples the examples to use
	 */
	protected void initializeCurrFromList(List<DataSet> examples) {
		
		if(examples.isEmpty())
			log.warn("Warning: empty dataset from the fetcher");
		curr = null;
		INDArray inputs = createInputMatrix(examples.size());
		INDArray labels = createOutputMatrix(examples.size());
		for(int i = 0; i < examples.size(); i++) {
			INDArray data = examples.get(i).getFeatureMatrix();
			INDArray label = examples.get(i).getLabels();
			inputs.putRow(i, data);
			labels.putRow(i,label);
		}
		curr = new DataSet(inputs,labels);
        examples.clear();

	}

    @Override
    public List<String> getLabels(){
        return null; }



}
