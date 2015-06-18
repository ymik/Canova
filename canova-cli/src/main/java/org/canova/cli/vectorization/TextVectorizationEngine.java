package org.canova.cli.vectorization;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.canova.api.io.data.DoubleWritable;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.records.reader.impl.FileRecordReader;
import org.canova.api.writable.Writable;
import org.canova.nd4j.nlp.vectorizer.TfidfVectorizer;
import org.nd4j.linalg.api.ndarray.INDArray;

public class TextVectorizationEngine extends VectorizationEngine {

	/**
	 * Currently the stock input format / RR gives us a vector already converted
	 * -	TODO: separate this into a transform plugin
	 * 
	 * 
	 * Thoughts
	 * 		-	Inside the vectorization engine is a great place to put a pluggable transformation system [ TODO: v2 ]
	 * 			-	example: MNIST binarization could be a pluggable transform
	 * 			-	example: custom thresholding on blocks of pixels
	 * 
	 * 
	 * Text Pipeline specific stuff
	 * 		-	so right now the TF-IDF stuff has 2 major issues
	 * 			1.	its not parallelizable in its current form (loading words into memory doesnt scale)
	 * 			2.	vectorization is embedded in the inputformat/recordreader - which is conflating functionality
	 * 
	 * 
	 */
	@Override
	public void execute() throws IOException {

		System.out.println( "TextVectorizationEngine > execute() [ START ]" );
		
		
		
        TfidfVectorizer vectorizer = new TfidfVectorizer();
     //   Configuration conf = new Configuration();
        conf.setInt(TfidfVectorizer.MIN_WORD_FREQUENCY, 1);
        conf.setBoolean(RecordReader.APPEND_LABEL, true);
        vectorizer.initialize(conf);        
		
        INDArray tfIdfVectors = null;
        
        FileRecordReader ref = (FileRecordReader)this.reader;
        
        List<String> labels = ref.getLabels();
        for ( int x = 0; x < labels.size(); x++ ) {
        	
        	System.out.println( "label: " + labels.get(x) );
        	
        }
        
        
		
        if (reader.hasNext()) {
            
        	// get the record from the input format
//        	Collection<Writable> w = reader.next();
        	
        	//w.size()
        	
 //       	System.out.println( "writeable: " + w.size() );
        	
        	tfIdfVectors = vectorizer.fitTransform(reader);
        	
            //number of vocab words is 3
 //           assertEquals(3,n.columns());
            //number of records is 2
 //           assertEquals(2,n.rows());        	
        	
        	// the reader did the work for us here
//        	writer.write(w);
 /*       	if (x > 10) {
        		break;
        	}
        	x++;
        	*/
        }
        
        Collection<Writable> outputVector = new ArrayList<>();
        
        System.out.println( "TF-IDF Rows: " + tfIdfVectors.rows() );
        System.out.println( "TF-IDF Cols: " + tfIdfVectors.columns() );
        
        for (int x = 0; x < tfIdfVectors.rows(); x++ ) {
        	
        	for (int col = 0; col < tfIdfVectors.columns(); col++ ) {
        
        		DoubleWritable d = new DoubleWritable();
        		d.set( tfIdfVectors.getDouble(x, col) );
        		outputVector.add( d );
        		
        		
        	}
        	
        	writer.write(outputVector);
        		
        	outputVector.clear();
        	
        }

        reader.close();
        writer.close();				
		
		
		System.out.println( "TextVectorizationEngine > execute() [ END ]" );
		
	}
	
	
}
