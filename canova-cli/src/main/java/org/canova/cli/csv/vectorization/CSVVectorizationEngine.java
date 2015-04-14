package org.canova.cli.csv.vectorization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;
import org.canova.cli.csv.schema.CSVInputSchema;
import org.canova.cli.csv.schema.CSVSchemaColumn;
import org.canova.cli.csv.schema.CSVSchemaColumn.TransformType;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

/**
 * Vectorization Engine
 * - takes CSV input and converts it to a transformed vector output in a standard format
 * - uses the input CSV schema and the collected statistics from a pre-pass
 * 
 * @author josh
 *
 */
public class CSVVectorizationEngine {

	/**
	 * Use statistics collected from a previous pass to vectorize (or drop) each column
	 * 
	 * @return
	 */
	public INDArray vectorize( String key, String value, CSVInputSchema schema ) {
		
		//INDArray
		INDArray ret = this.createArray( schema.getTransformedVectorSize() );
		
		// TODO: this needs to be different (needs to be real vector representation
		//String outputVector = "";
		String[] columns = value.split( schema.delimiter );
				
		if ( columns[0].trim().equals("") ) {
			System.out.println("Skipping blank line");
			return null;
		}
		
		int srcColIndex = 0;
		int dstColIndex = 0;
		
		System.out.println( "> Engine.vectorize() ----- ");
		
		// scan through the columns in the schema / input csv data
		for (Map.Entry<String, CSVSchemaColumn> entry : schema.getColumnSchemas().entrySet()) {
			
			String colKey = entry.getKey();
		    CSVSchemaColumn colSchemaEntry = entry.getValue();
		    
		    // produce the per column transform based on stats
		    
		    if ( TransformType.SKIP == colSchemaEntry.transform ) {
		    	
		    	// dont append this to the output vector, skipping
		    	
		    } else {
		    
		    	System.out.println( " column value: " + columns[ srcColIndex ] );
		    	
		    	double convertedColumn = colSchemaEntry.transformColumnValue( columns[ srcColIndex ].trim() );
		    	// add this value to the output vector

		    	ret.putScalar(dstColIndex, convertedColumn);
		    	
		    	dstColIndex++;
		    	
		    }
		    
		    
		    
		    srcColIndex++;
		    
		}		
		
		
		return ret;
		
	}
	
	/**
	 * Use statistics collected from a previous pass to vectorize (or drop) each column
	 * 
	 * @return
	 */
	public Collection<Writable> vectorizeToWritable( String key, String value, CSVInputSchema schema ) {
		
		//INDArray
		//INDArray ret = this.createArray( schema.getTransformedVectorSize() );
		Collection<Writable> ret = new ArrayList<Writable>();
		
		// TODO: this needs to be different (needs to be real vector representation
		//String outputVector = "";
		String[] columns = value.split( schema.delimiter );
				
		if ( columns[0].trim().equals("") ) {
			System.out.println("Skipping blank line");
			return null;
		}
		
		int srcColIndex = 0;
		int dstColIndex = 0;
		
		System.out.println( "> Engine.vectorize() ----- ");
		
		// scan through the columns in the schema / input csv data
		for (Map.Entry<String, CSVSchemaColumn> entry : schema.getColumnSchemas().entrySet()) {
			
			String colKey = entry.getKey();
		    CSVSchemaColumn colSchemaEntry = entry.getValue();
		    
		    // produce the per column transform based on stats
		    
		    if ( TransformType.SKIP == colSchemaEntry.transform ) {
		    	
		    	// dont append this to the output vector, skipping
		    	
		    } else {
		    
		    	System.out.println( " column value: " + columns[ srcColIndex ] );
		    	
		    	double convertedColumn = colSchemaEntry.transformColumnValue( columns[ srcColIndex ].trim() );
		    	// add this value to the output vector

		    	//ret.putScalar(dstColIndex, convertedColumn);
		    	ret.add(new Text(convertedColumn + ""));
		    	
		    	dstColIndex++;
		    	
		    }
		    
		    
		    
		    srcColIndex++;
		    
		}		
		
		
		return ret;
		
	}	
	
	
	public INDArray createArray( int size ) {
		
		INDArray ret = Nd4j.create( size ); //cache.vocabWords().size());
		
		return ret;
		
	}
	
	
	
}
